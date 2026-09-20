//! The system boundary. Everything past this module trusts its types.
//!
//! One accepted connection is one process stream: newline-delimited JSON, one
//! `lightwatch_proto::Message` per line, `Hello` first. The handshake is
//! strict, because a stream whose first line is not a hello has no process to
//! attribute anything to. After it, a bad line is counted and skipped, so one
//! corrupted frame does not cost a UI the rest of a run.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use lightwatch_proto::{Message, SCHEMA_VERSION};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader};
use tokio::net::UnixListener;
use tracing::{debug, info, warn};

use crate::store::{ProcessId, Registry};

/// The socket the daemon listens on inside its socket directory.
pub const SOCKET_FILE: &str = "lightwatch.sock";

/// A line longer than this is a client that will never send a newline. The
/// daemon stops reading rather than growing a buffer on its behalf.
pub const MAX_LINE_BYTES: usize = 4 * 1024 * 1024;

/// Why a connection ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Closed before sending anything.
    Empty,
    /// The first line parsed, but was not a hello.
    HandshakeNotHello,
    /// The first line did not parse.
    HandshakeMalformed,
    /// The client speaks a protocol version this daemon does not.
    SchemaMismatch { got: u32, expected: u32 },
    LineTooLong,
    ReadFailed,
    /// The stream ran to its end. `frames` counts the frames accepted on this
    /// connection, `skipped` the lines that were not usable frames.
    Ended { process: ProcessId, frames: u64, skipped: u64 },
}

/// Where connections are accepted. `$LIGHTWATCH_SOCK_DIR` wins; otherwise
/// `$TMPDIR/lightwatch` on macOS and `$XDG_RUNTIME_DIR/lightwatch` elsewhere.
pub fn socket_dir() -> PathBuf {
    resolve_socket_dir(
        std::env::var_os("LIGHTWATCH_SOCK_DIR").map(PathBuf::from),
        std::env::var_os("TMPDIR").map(PathBuf::from),
        std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        cfg!(target_os = "macos"),
        std::env::temp_dir(),
    )
}

fn resolve_socket_dir(
    explicit: Option<PathBuf>,
    tmpdir: Option<PathBuf>,
    xdg_runtime: Option<PathBuf>,
    on_macos: bool,
    temp_dir: PathBuf,
) -> PathBuf {
    if let Some(explicit) = explicit {
        return explicit;
    }
    if on_macos {
        if let Some(tmpdir) = tmpdir {
            return tmpdir.join("lightwatch");
        }
    }
    match xdg_runtime {
        Some(runtime) => runtime.join("lightwatch"),
        None => temp_dir.join("lightwatch"),
    }
}

/// Creates the socket directory if it is missing and binds the listener.
///
/// A socket file outlives a daemon that crashed, so an `AddrInUse` is only
/// reclaimed when nothing answers on it. That keeps a second daemon from
/// stealing a running one's socket and silently splitting the feed.
pub fn bind(dir: &Path) -> std::io::Result<UnixListener> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(SOCKET_FILE);
    match UnixListener::bind(&path) {
        Err(err) if err.kind() == ErrorKind::AddrInUse => {
            if std::os::unix::net::UnixStream::connect(&path).is_ok() {
                return Err(std::io::Error::new(
                    ErrorKind::AddrInUse,
                    format!("another lightwatch is already listening on {}", path.display()),
                ));
            }
            std::fs::remove_file(&path)?;
            UnixListener::bind(&path)
        }
        other => other,
    }
}

pub async fn accept_loop(listener: UnixListener, registry: Arc<Registry>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let registry = Arc::clone(&registry);
                tokio::spawn(async move {
                    let outcome = handle_connection(stream, registry).await;
                    debug!(?outcome, "stream closed");
                });
            }
            Err(err) => {
                warn!(%err, "accept failed");
            }
        }
    }
}

pub async fn handle_connection<S: AsyncRead + Unpin>(stream: S, registry: Arc<Registry>) -> Outcome {
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();

    let hello = match read_line(&mut reader, &mut line).await {
        LineRead::Eof => return Outcome::Empty,
        LineRead::TooLong => {
            warn!("refused a stream whose handshake exceeded {MAX_LINE_BYTES} bytes");
            return Outcome::LineTooLong;
        }
        LineRead::Failed => return Outcome::ReadFailed,
        LineRead::Line(text) => match Message::from_json_line(&text) {
            Ok(Message::Hello(hello)) => hello,
            Ok(Message::Frame(_)) => {
                warn!("refused a stream that opened with a frame instead of a hello");
                return Outcome::HandshakeNotHello;
            }
            Err(err) => {
                warn!(%err, "refused a stream whose handshake did not parse");
                return Outcome::HandshakeMalformed;
            }
        },
    };

    if hello.schema != SCHEMA_VERSION {
        warn!(
            app = %hello.app,
            pid = hello.pid,
            source = %hello.source,
            got = hello.schema,
            expected = SCHEMA_VERSION,
            "refused a stream speaking another schema version"
        );
        return Outcome::SchemaMismatch { got: hello.schema, expected: SCHEMA_VERSION };
    }

    let state = registry.connect(&hello);
    let id = state.read().expect("process lock").id.clone();
    info!(process = %id, app = %hello.app, pid = hello.pid, source = %hello.source, "stream opened");

    let mut frames = 0u64;
    let mut skipped = 0u64;
    let outcome = loop {
        match read_line(&mut reader, &mut line).await {
            LineRead::Eof => break Outcome::Ended { process: id.clone(), frames, skipped },
            LineRead::Failed => break Outcome::ReadFailed,
            LineRead::TooLong => {
                warn!(process = %id, "closing a stream that sent a line over {MAX_LINE_BYTES} bytes");
                break Outcome::LineTooLong;
            }
            LineRead::Line(text) => {
                if text.trim().is_empty() {
                    continue;
                }
                match Message::from_json_line(&text) {
                    Ok(Message::Frame(frame)) => {
                        frames += 1;
                        state.write().expect("process lock").apply_frame(&frame);
                    }
                    Ok(Message::Hello(_)) => {
                        skipped += 1;
                        warn!(process = %id, "skipped a second hello mid-stream");
                        state.write().expect("process lock").note_malformed_line();
                    }
                    Err(err) => {
                        skipped += 1;
                        warn!(process = %id, %err, "skipped a line that did not parse");
                        state.write().expect("process lock").note_malformed_line();
                    }
                }
            }
        }
    };

    registry.disconnect(&id);
    info!(process = %id, frames, skipped, "stream ended");
    outcome
}

enum LineRead {
    Line(String),
    Eof,
    TooLong,
    Failed,
}

/// Reads one newline-terminated line, refusing to buffer more than
/// [`MAX_LINE_BYTES`]. A line that is not UTF-8 comes back as one that will not
/// parse, so it takes the malformed-line path rather than a separate one.
async fn read_line<R: AsyncBufRead + Unpin>(reader: &mut R, buffer: &mut Vec<u8>) -> LineRead {
    buffer.clear();
    loop {
        let available = match reader.fill_buf().await {
            Ok(bytes) => bytes,
            Err(_) => return LineRead::Failed,
        };
        if available.is_empty() {
            return if buffer.is_empty() {
                LineRead::Eof
            } else {
                LineRead::Line(String::from_utf8_lossy(buffer).into_owned())
            };
        }
        match available.iter().position(|byte| *byte == b'\n') {
            Some(newline) => {
                buffer.extend_from_slice(&available[..newline]);
                reader.consume(newline + 1);
                if buffer.len() > MAX_LINE_BYTES {
                    return LineRead::TooLong;
                }
                return LineRead::Line(String::from_utf8_lossy(buffer).into_owned());
            }
            None => {
                let consumed = available.len();
                buffer.extend_from_slice(available);
                reader.consume(consumed);
                if buffer.len() > MAX_LINE_BYTES {
                    return LineRead::TooLong;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightwatch_proto::{Dist, Event, Frame, FunctionId, Hello, Register};

    fn hello_line() -> String {
        Message::Hello(Hello::new("demo", "test", 4242, 1_700_000_000_000)).to_json_line()
    }

    fn register_line() -> String {
        Message::Frame(Frame {
            seq: 0,
            t_ns: 0,
            registers: vec![Register::Function {
                id: FunctionId(1),
                name: "decode".into(),
                module: None,
                location: None,
            }],
            events: vec![],
        })
        .to_json_line()
    }

    fn calls_line(seq: u64, t_ns: u64, count: u64) -> String {
        Message::Frame(Frame {
            seq,
            t_ns,
            registers: vec![],
            events: vec![Event::Calls {
                func: FunctionId(1),
                count,
                ns: Dist::Raw { v: vec![1_000; count as usize] },
            }],
        })
        .to_json_line()
    }

    async fn feed(lines: &[String]) -> (Arc<Registry>, Outcome) {
        let registry = Arc::new(Registry::new());
        let stream = lines.join("\n").into_bytes();
        let outcome = handle_connection(std::io::Cursor::new(stream), Arc::clone(&registry)).await;
        (registry, outcome)
    }

    fn demo_id() -> ProcessId {
        ProcessId::of(4242, 1_700_000_000_000)
    }

    #[tokio::test]
    async fn a_malformed_line_is_counted_and_skipped_without_killing_the_connection() {
        let (registry, outcome) = feed(&[
            hello_line(),
            register_line(),
            "{ this is not json".to_string(),
            calls_line(1, 100_000_000, 3),
            r#"{"msg":"frame""#.to_string(),
            calls_line(2, 200_000_000, 4),
            String::new(),
        ])
        .await;

        assert_eq!(outcome, Outcome::Ended { process: demo_id(), frames: 3, skipped: 2 });
        let state = registry.get(&demo_id()).expect("the process was registered");
        let process = state.read().unwrap();
        assert_eq!(process.counts.malformed_lines, 2);
        assert_eq!(
            process.functions[&FunctionId(1)].calls.get(),
            7,
            "the frames on either side of the bad lines both landed"
        );
    }

    #[tokio::test]
    async fn a_schema_mismatch_is_refused_and_registers_nothing() {
        let bad = r#"{"msg":"hello","schema":99,"app":"demo","pid":4242,"started_unix_ms":1,"window_ms":100,"source":"test"}"#;
        let (registry, outcome) = feed(&[bad.to_string(), register_line(), String::new()]).await;

        assert_eq!(outcome, Outcome::SchemaMismatch { got: 99, expected: SCHEMA_VERSION });
        assert!(registry.is_empty(), "a refused stream leaves no process behind");
    }

    #[tokio::test]
    async fn a_stream_that_opens_with_a_frame_is_refused() {
        let (registry, outcome) = feed(&[register_line(), String::new()]).await;
        assert_eq!(outcome, Outcome::HandshakeNotHello);
        assert!(registry.is_empty());
    }

    #[tokio::test]
    async fn a_handshake_that_does_not_parse_is_refused() {
        let (registry, outcome) = feed(&["garbage".to_string(), String::new()]).await;
        assert_eq!(outcome, Outcome::HandshakeMalformed);
        assert!(registry.is_empty());
    }

    #[tokio::test]
    async fn a_client_that_never_sends_a_newline_is_cut_off_rather_than_buffered_forever() {
        let registry = Arc::new(Registry::new());
        let mut stream = hello_line().into_bytes();
        stream.push(b'\n');
        stream.extend(std::iter::repeat_n(b'x', MAX_LINE_BYTES + 1));
        let outcome =
            handle_connection(std::io::Cursor::new(stream), Arc::clone(&registry)).await;
        assert_eq!(outcome, Outcome::LineTooLong);
    }

    #[tokio::test]
    async fn an_unknown_id_arriving_over_the_wire_is_counted_and_dropped() {
        let unknown = Message::Frame(Frame {
            seq: 1,
            t_ns: 100_000_000,
            registers: vec![],
            events: vec![Event::Calls { func: FunctionId(77), count: 9, ns: Dist::empty() }],
        })
        .to_json_line();
        let (registry, _) = feed(&[hello_line(), register_line(), unknown, String::new()]).await;

        let state = registry.get(&demo_id()).expect("the process was registered");
        let process = state.read().unwrap();
        assert_eq!(process.counts.unknown_id, 1);
        assert_eq!(process.counts.malformed_lines, 0, "an unknown id is not a malformed line");
        assert!(!process.functions.contains_key(&FunctionId(77)));
    }

    #[tokio::test]
    async fn a_closed_stream_leaves_the_process_visible_and_ended() {
        let (registry, _) =
            feed(&[hello_line(), register_line(), calls_line(1, 100_000_000, 5), String::new()])
                .await;
        let state = registry.get(&demo_id()).expect("still listed after the stream closed");
        let process = state.read().unwrap();
        assert!(process.ended_unix_ms.is_some());
        assert_eq!(process.functions[&FunctionId(1)].calls.get(), 5);
    }

    #[tokio::test]
    async fn two_streams_from_different_processes_are_kept_apart() {
        let registry = Arc::new(Registry::new());
        let first = [hello_line(), register_line(), calls_line(1, 100_000_000, 5)].join("\n");
        let other_hello =
            Message::Hello(Hello::new("other", "test", 99, 1_700_000_000_001)).to_json_line();
        let second = [other_hello, register_line(), calls_line(1, 100_000_000, 2)].join("\n");

        let (a, b) = tokio::join!(
            handle_connection(std::io::Cursor::new(first.into_bytes()), Arc::clone(&registry)),
            handle_connection(std::io::Cursor::new(second.into_bytes()), Arc::clone(&registry)),
        );
        assert!(matches!(a, Outcome::Ended { .. }));
        assert!(matches!(b, Outcome::Ended { .. }));
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry.get(&demo_id()).unwrap().read().unwrap().functions[&FunctionId(1)].calls.get(),
            5
        );
        assert_eq!(
            registry
                .get(&ProcessId::of(99, 1_700_000_000_001))
                .unwrap()
                .read()
                .unwrap()
                .functions[&FunctionId(1)]
                .calls
                .get(),
            2
        );
    }

    #[test]
    fn the_socket_directory_prefers_the_explicit_override_then_the_platform_default() {
        let explicit = Some(PathBuf::from("/run/chosen"));
        let tmpdir = Some(PathBuf::from("/var/tmp-per-user"));
        let xdg = Some(PathBuf::from("/run/user/501"));
        let fallback = PathBuf::from("/tmp");

        assert_eq!(
            resolve_socket_dir(explicit.clone(), tmpdir.clone(), xdg.clone(), true, fallback.clone()),
            PathBuf::from("/run/chosen")
        );
        assert_eq!(
            resolve_socket_dir(None, tmpdir.clone(), xdg.clone(), true, fallback.clone()),
            PathBuf::from("/var/tmp-per-user/lightwatch")
        );
        assert_eq!(
            resolve_socket_dir(None, tmpdir, xdg.clone(), false, fallback.clone()),
            PathBuf::from("/run/user/501/lightwatch")
        );
        assert_eq!(
            resolve_socket_dir(None, None, None, false, fallback),
            PathBuf::from("/tmp/lightwatch")
        );
    }
}
