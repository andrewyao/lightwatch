//! The background thread that turns accumulators into frames.
//!
//! It owns the socket. A measured thread never touches it, never waits on it,
//! and never learns whether anyone is listening. When no daemon is there the
//! window is drained and thrown away, which keeps memory flat while the
//! program runs unobserved, and the next window tries to connect again.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lightwatch_proto::{Event, Frame, Hello, Message, DEFAULT_WINDOW_MS, SOCKET_FILE};

use crate::calls;
use crate::census::bytes_from_buckets;
use crate::registry;

pub(crate) const SOURCE: &str = "lightwatch-probe";

static RUNNING: AtomicBool = AtomicBool::new(false);

/// Starts emitting, naming the process after its executable.
pub fn start() {
    let app = std::env::current_exe()
        .ok()
        .and_then(|path| path.file_stem().map(|stem| stem.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "unknown".to_string());
    start_named(app);
}

/// Starts emitting under a chosen name. Calling it twice does nothing the
/// second time.
pub fn start_named(app: impl Into<String>) {
    if RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }
    let app = app.into();
    let window = Duration::from_millis(window_ms() as u64);
    std::thread::Builder::new()
        .name("lightwatch-emit".to_string())
        .spawn(move || Emitter::new(app).run(window))
        .expect("the emit thread is the only thread lightwatch spawns");
}

fn window_ms() -> u32 {
    std::env::var("LIGHTWATCH_WINDOW_MS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_WINDOW_MS)
}

struct Emitter {
    hello: Hello,
    start: Instant,
    seq: u64,
    sink: Option<Sink>,
    functions_sent: usize,
    types_sent: usize,
    paths_sent: usize,
}

impl Emitter {
    fn new(app: String) -> Self {
        let started_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_millis() as u64)
            .unwrap_or(0);
        let mut hello = Hello::new(app, SOURCE, std::process::id(), started_unix_ms);
        hello.window_ms = window_ms();
        Emitter {
            hello,
            start: Instant::now(),
            seq: 0,
            sink: None,
            functions_sent: 0,
            types_sent: 0,
            paths_sent: 0,
        }
    }

    fn run(mut self, window: Duration) {
        loop {
            std::thread::sleep(window);
            self.tick();
        }
    }

    fn tick(&mut self) {
        // Drained whether or not anyone is listening, so an unobserved process
        // does not grow a window's worth of counters forever.
        let accum = calls::drain();

        if self.sink.is_none() {
            match Sink::connect() {
                Some(mut sink) => {
                    if sink.send(&Message::Hello(self.hello.clone())).is_err() {
                        return;
                    }
                    // A fresh listener has never seen this process's names.
                    self.functions_sent = 0;
                    self.types_sent = 0;
                    self.paths_sent = 0;
                    self.sink = Some(sink);
                }
                None => return,
            }
        }

        let (registers, functions_sent, types_sent, paths_sent) =
            registry::registers_since(self.functions_sent, self.types_sent, self.paths_sent);

        let mut events = Vec::new();
        for (func, stat) in accum.calls {
            events.push(Event::Calls {
                func: lightwatch_proto::FunctionId(func),
                count: stat.count,
                ns: stat.ns.into_dist(),
            });
        }
        for (path, stat) in accum.stacks {
            events.push(Event::Stack {
                path: lightwatch_proto::PathId(path),
                count: stat.count,
                self_ns: stat.self_ns,
            });
        }
        for (id, slot) in registry::tracked_types() {
            let sizes = slot.size_buckets();
            events.push(Event::Census {
                ty: id,
                live: slot.live(),
                bytes: bytes_from_buckets(&sizes),
                sizes: lightwatch_proto::Dist::Buckets { b: sizes },
            });
        }

        let frame = Frame {
            seq: self.seq,
            t_ns: self.start.elapsed().as_nanos() as u64,
            registers,
            events,
        };

        let sent = self
            .sink
            .as_mut()
            .map(|sink| sink.send(&Message::Frame(frame)))
            .unwrap_or(Ok(()));
        match sent {
            Ok(()) => {
                self.seq += 1;
                self.functions_sent = functions_sent;
                self.types_sent = types_sent;
                self.paths_sent = paths_sent;
            }
            // The daemon went away. Reconnecting re-sends the hello and every
            // name binding, so the next listener is not left with bare ids.
            Err(_) => self.sink = None,
        }
    }
}

/// Where the probe looks for a daemon, in order.
///
/// `LIGHTWATCH_SOCK` names one socket exactly. Otherwise the directory holds
/// `lightwatch.sock` by convention, and any other socket in it is tried after
/// that so a daemon listening under a different name is still found.
pub(crate) fn candidate_sockets(dir: &Path, explicit: Option<&str>) -> Vec<PathBuf> {
    if let Some(path) = explicit {
        return vec![PathBuf::from(path)];
    }
    let mut candidates = vec![dir.join(SOCKET_FILE)];
    if let Ok(entries) = std::fs::read_dir(dir) {
        let mut others: Vec<PathBuf> = entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension().is_some_and(|ext| ext == "sock") && *path != candidates[0]
            })
            .collect();
        others.sort();
        candidates.extend(others);
    }
    candidates
}

fn configured_sockets() -> Vec<PathBuf> {
    candidate_sockets(
        &lightwatch_proto::socket_dir(),
        std::env::var("LIGHTWATCH_SOCK").ok().as_deref(),
    )
}

#[cfg(unix)]
struct Sink {
    stream: std::io::BufWriter<std::os::unix::net::UnixStream>,
}

#[cfg(unix)]
impl Sink {
    fn connect() -> Option<Self> {
        for path in configured_sockets() {
            let Ok(stream) = std::os::unix::net::UnixStream::connect(&path) else { continue };
            // A wedged reader must not stall the emit thread indefinitely; a
            // failed write drops the connection and the next window retries.
            let _ = stream.set_write_timeout(Some(Duration::from_millis(250)));
            return Some(Sink { stream: std::io::BufWriter::new(stream) });
        }
        None
    }

    fn send(&mut self, message: &Message) -> std::io::Result<()> {
        self.stream.write_all(message.to_json_line().as_bytes())?;
        self.stream.write_all(b"\n")?;
        self.stream.flush()
    }
}

#[cfg(not(unix))]
struct Sink;

#[cfg(not(unix))]
impl Sink {
    fn connect() -> Option<Self> {
        None
    }

    fn send(&mut self, _message: &Message) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_socket_path_is_the_only_candidate() {
        assert_eq!(
            candidate_sockets(Path::new("/run/lw"), Some("/run/other.sock")),
            vec![PathBuf::from("/run/other.sock")]
        );
    }

    #[test]
    fn the_conventional_socket_is_tried_first() {
        let dir = std::env::temp_dir().join("lightwatch-candidate-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(dir.join("zzz.sock"), b"").expect("decoy socket file");
        let found = candidate_sockets(&dir, None);
        assert_eq!(found[0], dir.join(SOCKET_FILE));
        assert!(found.contains(&dir.join("zzz.sock")), "other sockets stay reachable");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
