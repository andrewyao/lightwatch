//! `lightwatch-hotpath`: poll a hotpath-profiled process, feed the daemon.
//!
//! See the crate documentation in `lib.rs` for what this source can and cannot
//! report. Nothing in the loop below is allowed to end the process: a target
//! that has not started, has died, or has been restarted, and a daemon socket
//! that is not there, are all states the bridge waits out.

use clap::Parser;
use lightwatch_hotpath::bridge::{app_name, Stream};
use lightwatch_hotpath::sink::{Connection, DEFAULT_SOCKET};
use lightwatch_hotpath::source::{PollError, Target};
use lightwatch_proto::{Hello, DEFAULT_WINDOW_MS};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Longest wait between retries while the target or the daemon is away.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[command(
    version,
    about = "Stream a hotpath-profiled process into lightwatch, without modifying it"
)]
struct Args {
    /// Host running the profiled process.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port of the profiled process's hotpath metrics server.
    #[arg(long, default_value_t = 6770)]
    port: u16,

    /// Milliseconds covered by one frame.
    #[arg(long, default_value_t = DEFAULT_WINDOW_MS)]
    window_ms: u32,

    /// Daemon ingest socket.
    #[arg(long, default_value = DEFAULT_SOCKET)]
    socket: PathBuf,

    /// Milliseconds to wait on the target before giving up on one poll.
    #[arg(long, default_value_t = 2000)]
    timeout_ms: u64,
}

/// One connection to the daemon, describing one run of one target process.
/// Both halves are born and discarded together: a reopened connection needs
/// the registrations again, and those live in the stream.
struct Session {
    connection: Connection,
    stream: Stream,
}

enum Trouble {
    Target(PollError),
    Daemon(std::io::Error),
}

impl std::fmt::Display for Trouble {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Trouble::Target(e) => write!(f, "{e}"),
            Trouble::Daemon(e) => write!(f, "daemon socket unavailable: {e}"),
        }
    }
}

fn main() {
    let args = Args::parse();
    let target = Target::new(
        args.host.clone(),
        args.port,
        Duration::from_millis(args.timeout_ms),
    );
    let window = Duration::from_millis(args.window_ms as u64);

    eprintln!(
        "lightwatch-hotpath: {} -> {}, {} ms windows. Degraded source: calls and durations only, no edges and no census.",
        target.address(),
        args.socket.display(),
        args.window_ms
    );

    let mut session = None;
    let mut backoff = window;
    let mut log = Log::default();
    loop {
        let started = Instant::now();
        match poll_once(&target, &args, session.take()) {
            Ok(open) => {
                session = Some(open);
                backoff = window;
                log.recovered();
                std::thread::sleep(window.saturating_sub(started.elapsed()));
            }
            Err(trouble) => {
                log.trouble(trouble.to_string());
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Reads the target once and sends the frame it implies. A session that comes
/// back out is good for the next window; anything else surfaces as an error
/// and drops the session with it.
fn poll_once(target: &Target, args: &Args, session: Option<Session>) -> Result<Session, Trouble> {
    let status = target.status().map_err(Trouble::Target)?;
    let report = target.functions_timing().map_err(Trouble::Target)?;

    let mut session = match session {
        Some(open) if open.stream.same_run(status.pid, report.total_elapsed_ns) => open,
        Some(_) => {
            eprintln!(
                "target restarted as pid {}, opening a new stream",
                status.pid
            );
            open_session(args, status.pid, &report)?
        }
        None => open_session(args, status.pid, &report)?,
    };

    let frame = session.stream.absorb(&report);
    session.connection.send(frame).map_err(Trouble::Daemon)?;
    Ok(session)
}

fn open_session(
    args: &Args,
    pid: u32,
    report: &hotpath::json::JsonFunctionsList,
) -> Result<Session, Trouble> {
    let app = app_name(&report.caller_name);
    let mut hello = Hello::new(
        app,
        "lightwatch-hotpath",
        pid,
        process_start_unix_ms(report.total_elapsed_ns),
    );
    hello.window_ms = args.window_ms;
    let connection = Connection::open(&args.socket, &hello).map_err(Trouble::Daemon)?;
    eprintln!("streaming {app} (pid {pid}) to {}", args.socket.display());
    Ok(Session {
        connection,
        stream: Stream::new(pid),
    })
}

/// hotpath reports elapsed time since its own start, which is close enough to
/// process start to tell a recycled pid from a reused one.
fn process_start_unix_ms(elapsed_ns: u64) -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    now_ms.saturating_sub(elapsed_ns / 1_000_000)
}

/// Keeps a target that is down for an hour from writing a line every window.
#[derive(Default)]
struct Log {
    complaint: Option<String>,
}

impl Log {
    fn trouble(&mut self, message: String) {
        if self.complaint.as_deref() != Some(message.as_str()) {
            eprintln!("waiting: {message}");
            self.complaint = Some(message);
        }
    }

    fn recovered(&mut self) {
        if self.complaint.take().is_some() {
            eprintln!("recovered");
        }
    }
}
