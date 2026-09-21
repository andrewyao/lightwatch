//! The JSON the web UI reads. Every shape here is a published contract, so the
//! field names spell out their units and say whether a number accumulates or
//! is a reading at an instant.

use lightwatch_proto::bucket_range;
use serde::Serialize;

use crate::ring::{CensusReading, Window};
use crate::session::{Feed, Member, Session};
use crate::store::{Counts, ProcessId, ProcessState, Update};

/// How much of the recent past a per-second rate is averaged over.
pub const RATE_WINDOW_MS: u64 = 1_000;

#[derive(Debug, Serialize)]
pub struct ProcessList {
    pub processes: Vec<ProcessSummary>,
}

#[derive(Debug, Serialize)]
pub struct ProcessSummary {
    pub id: String,
    pub app: String,
    pub pid: u32,
    pub source: String,
    pub started_unix_ms: u64,
    /// Set once the last connection for this process closes. A null here means
    /// the process is still streaming.
    pub ended_unix_ms: Option<u64>,
    pub connected: bool,
    /// The window length the emitter declared, which is not the daemon's own
    /// window length. See `fine_window_ms` on a snapshot.
    pub window_ms: u32,
    pub last_frame_unix_ms: Option<u64>,
    /// Monotonic nanoseconds since process start, at the newest frame's close.
    pub last_t_ns: u64,
    pub counts: CountsJson,
}

#[derive(Debug, Serialize)]
pub struct CountsJson {
    pub frames: u64,
    pub events: u64,
    pub functions: u64,
    pub types: u64,
    pub edges: u64,
    pub missed_frames: u64,
    pub late_frames: u64,
    pub unknown_id: u64,
    pub malformed_lines: u64,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub process: ProcessSummary,
    pub taken_unix_ms: u64,
    pub rate_window_ms: u64,
    pub fine_window_ms: u64,
    pub fine_window_capacity: u64,
    pub functions: Vec<FunctionJson>,
    pub types: Vec<TypeJson>,
    pub edges: Vec<EdgeJson>,
    pub windows: Vec<WindowJson>,
}

#[derive(Debug, Serialize)]
pub struct FunctionJson {
    pub id: u32,
    pub name: String,
    pub module: Option<String>,
    pub location: Option<LocationJson>,
    /// Calls since the process connected. Accumulates.
    pub calls_total: u64,
    /// Nanoseconds spent in those calls, since the process connected.
    pub ns_total: u64,
    pub calls_per_sec: f64,
    pub ns_per_sec: f64,
    /// Lifetime duration histogram. `lo` and `hi` are nanoseconds.
    pub ns_buckets: Vec<BucketJson>,
}

#[derive(Debug, Serialize)]
pub struct TypeJson {
    pub id: u32,
    pub name: String,
    pub location: Option<LocationJson>,
    /// The latest reading, or null if this type has not reported one yet.
    pub census: Option<CensusJson>,
}

/// A reading at one instant. These numbers replace the previous reading; they
/// are never summed over time.
#[derive(Debug, Serialize)]
pub struct CensusJson {
    pub live: u64,
    pub bytes: u64,
    pub at_t_ns: u64,
    /// Sizes of the live instances. `lo` and `hi` are bytes.
    pub size_buckets: Vec<BucketJson>,
}

#[derive(Debug, Serialize)]
pub struct EdgeJson {
    pub from: u32,
    pub to: u32,
    /// Calls since the process connected. Never expires.
    pub calls_total: u64,
}

#[derive(Debug, Serialize)]
pub struct BucketJson {
    pub lo: u64,
    pub hi: u64,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct LocationJson {
    pub file: String,
    pub line: u32,
    pub column: Option<u32>,
}

/// One closed window. `calls` and `edges` are that window's own deltas;
/// `census` is the reading taken at its close.
#[derive(Debug, Serialize)]
pub struct WindowJson {
    pub index: u64,
    pub start_t_ns: u64,
    pub end_t_ns: u64,
    pub calls: Vec<WindowCallJson>,
    pub edges: Vec<WindowEdgeJson>,
    pub census: Vec<WindowCensusJson>,
}

#[derive(Debug, Serialize)]
pub struct WindowCallJson {
    pub func: u32,
    pub calls: u64,
    pub ns: u64,
}

#[derive(Debug, Serialize)]
pub struct WindowEdgeJson {
    pub from: u32,
    pub to: u32,
    pub calls: u64,
}

#[derive(Debug, Serialize)]
pub struct WindowCensusJson {
    pub ty: u32,
    pub live: u64,
    pub bytes: u64,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamMessage {
    Window { process_id: String, window: WindowJson },
    ProcessEnded { process_id: String, ended_unix_ms: u64 },
}

#[derive(Debug, Serialize)]
pub struct SessionList {
    pub sessions: Vec<SessionJson>,
}

/// One program, and the emitters watching it. `cpu` and `memory` each name a
/// process id the existing `/api/processes/{id}/snapshot` route accepts.
#[derive(Debug, Serialize)]
pub struct SessionJson {
    pub id: String,
    pub app: String,
    pub pid: u32,
    pub connected: bool,
    pub cpu: Option<MemberJson>,
    pub memory: Option<MemberJson>,
    /// Entries that matched this session and lost to a better one of their own
    /// feed. Usually a bridge restarted against a target that never died.
    pub superseded: Vec<MemberJson>,
}

#[derive(Debug, Serialize)]
pub struct MemberJson {
    pub process_id: String,
    pub feed: &'static str,
    pub source: String,
    pub started_unix_ms: u64,
    pub ended_unix_ms: Option<u64>,
    pub connected: bool,
    pub last_frame_unix_ms: Option<u64>,
}

/// A stream message with the feed it came from, since one session's socket
/// carries both.
#[derive(Debug, Serialize)]
pub struct SessionStreamMessage {
    pub session_id: String,
    pub feed: &'static str,
    #[serde(flatten)]
    pub message: StreamMessage,
}

pub fn summary(process: &ProcessState) -> ProcessSummary {
    ProcessSummary {
        id: process.id.to_string(),
        app: process.app.clone(),
        pid: process.pid,
        source: process.source.clone(),
        started_unix_ms: process.started_unix_ms,
        ended_unix_ms: process.ended_unix_ms,
        connected: process.is_connected(),
        window_ms: process.window_ms,
        last_frame_unix_ms: process.last_frame_unix_ms,
        last_t_ns: process.last_t_ns,
        counts: counts(process),
    }
}

fn counts(process: &ProcessState) -> CountsJson {
    let Counts { frames, events, unknown_id, malformed_lines, missed_frames, late_frames } =
        process.counts;
    CountsJson {
        frames,
        events,
        functions: process.functions.len() as u64,
        types: process.types.len() as u64,
        edges: process.edges.len() as u64,
        missed_frames,
        late_frames,
        unknown_id,
        malformed_lines,
    }
}

pub fn snapshot(process: &ProcessState, taken_unix_ms: u64) -> Snapshot {
    let resolution_ns = process.fine.resolution_ns();
    let rate_windows = (RATE_WINDOW_MS * 1_000_000 / resolution_ns).max(1) as usize;
    let recent: Vec<&Window> = process.fine.most_recent(rate_windows).collect();
    let covered_secs = (recent.len() as f64 * resolution_ns as f64) / 1e9;

    let functions = process
        .functions
        .iter()
        .map(|(id, state)| {
            let (calls, ns) = recent.iter().fold((0u64, 0u64), |(calls, ns), window| {
                match window.calls.get(id) {
                    Some(delta) => (calls + delta.calls.get(), ns + delta.ns.get()),
                    None => (calls, ns),
                }
            });
            FunctionJson {
                id: id.0,
                name: state.name.clone(),
                module: state.module.clone(),
                location: state.location.as_ref().map(location),
                calls_total: state.calls.get(),
                ns_total: state.ns_total.get(),
                calls_per_sec: per_sec(calls, covered_secs),
                ns_per_sec: per_sec(ns, covered_secs),
                ns_buckets: state
                    .ns_buckets
                    .iter()
                    .map(|(index, count)| bucket(*index, *count))
                    .collect(),
            }
        })
        .collect();

    let types = process
        .types
        .iter()
        .map(|(id, state)| TypeJson {
            id: id.0,
            name: state.name.clone(),
            location: state.location.as_ref().map(location),
            census: state.latest.as_ref().map(census),
        })
        .collect();

    let edges = process
        .edges
        .iter()
        .map(|((from, to), calls)| EdgeJson { from: from.0, to: to.0, calls_total: calls.get() })
        .collect();

    Snapshot {
        process: summary(process),
        taken_unix_ms,
        rate_window_ms: RATE_WINDOW_MS,
        fine_window_ms: resolution_ns / 1_000_000,
        fine_window_capacity: process.fine.capacity() as u64,
        functions,
        types,
        edges,
        windows: process.fine.iter().map(window).collect(),
    }
}

pub fn session_list(sessions: Vec<Session>) -> SessionList {
    SessionList { sessions: sessions.into_iter().map(session).collect() }
}

pub fn session(source: Session) -> SessionJson {
    SessionJson {
        id: source.id.clone(),
        connected: source.is_connected(),
        app: source.app,
        pid: source.pid,
        cpu: source.cpu.map(member),
        memory: source.memory.map(member),
        superseded: source.superseded.into_iter().map(member).collect(),
    }
}

fn member(source: Member) -> MemberJson {
    MemberJson {
        process_id: source.process_id.to_string(),
        feed: source.feed.as_str(),
        source: source.source,
        started_unix_ms: source.started_unix_ms,
        ended_unix_ms: source.ended_unix_ms,
        connected: source.connected,
        last_frame_unix_ms: source.last_frame_unix_ms,
    }
}

pub fn session_stream_message(
    session_id: &str,
    feed: Feed,
    process_id: &ProcessId,
    update: &Update,
) -> SessionStreamMessage {
    SessionStreamMessage {
        session_id: session_id.to_string(),
        feed: feed.as_str(),
        message: stream_message(process_id, update),
    }
}

pub fn stream_message(id: &ProcessId, update: &Update) -> StreamMessage {
    match update {
        Update::WindowClosed(closed) => {
            StreamMessage::Window { process_id: id.to_string(), window: window(closed) }
        }
        Update::ProcessEnded { ended_unix_ms } => {
            StreamMessage::ProcessEnded { process_id: id.to_string(), ended_unix_ms: *ended_unix_ms }
        }
    }
}

fn window(source: &Window) -> WindowJson {
    WindowJson {
        index: source.index,
        start_t_ns: source.start_t_ns(),
        end_t_ns: source.end_t_ns(),
        calls: source
            .calls
            .iter()
            .map(|(func, delta)| WindowCallJson {
                func: func.0,
                calls: delta.calls.get(),
                ns: delta.ns.get(),
            })
            .collect(),
        edges: source
            .edges
            .iter()
            .map(|((from, to), calls)| WindowEdgeJson { from: from.0, to: to.0, calls: calls.get() })
            .collect(),
        census: source
            .census
            .iter()
            .map(|(ty, reading)| WindowCensusJson {
                ty: ty.0,
                live: reading.live.get(),
                bytes: reading.bytes.get(),
            })
            .collect(),
    }
}

fn census(reading: &CensusReading) -> CensusJson {
    CensusJson {
        live: reading.live.get(),
        bytes: reading.bytes.get(),
        at_t_ns: reading.at_t_ns,
        size_buckets: reading
            .size_buckets
            .iter()
            .map(|(index, count)| bucket(*index, *count as u64))
            .collect(),
    }
}

fn bucket(index: u16, count: u64) -> BucketJson {
    let (lo, hi) = bucket_range(index);
    BucketJson { lo, hi, count }
}

fn location(source: &lightwatch_proto::Location) -> LocationJson {
    LocationJson { file: source.file.clone(), line: source.line, column: source.column }
}

fn per_sec(total: u64, covered_secs: f64) -> f64 {
    if covered_secs <= 0.0 {
        return 0.0;
    }
    total as f64 / covered_secs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Registry;
    use lightwatch_proto::{Dist, Event, Frame, FunctionId, Hello, Register, TypeId};

    fn process_with_traffic() -> std::sync::Arc<std::sync::RwLock<ProcessState>> {
        let registry = Registry::new();
        let state = registry.connect(&Hello::new("demo", "test", 4242, 1_700_000_000_000));
        let mut process = state.write().unwrap();
        process.apply_frame(&Frame {
            seq: 0,
            t_ns: 0,
            registers: vec![
                Register::Function {
                    id: FunctionId(1),
                    name: "decode".into(),
                    module: Some("demo".into()),
                    location: None,
                },
                Register::Function {
                    id: FunctionId(2),
                    name: "resize".into(),
                    module: None,
                    location: None,
                },
                Register::Type { id: TypeId(1), name: "Thumbnail".into(), location: None },
            ],
            events: vec![],
        });
        for window in 1..=10u64 {
            process.apply_frame(&Frame {
                seq: window,
                t_ns: window * 100_000_000,
                registers: vec![],
                events: vec![
                    Event::Calls {
                        func: FunctionId(1),
                        count: 4,
                        ns: Dist::Raw { v: vec![1_000_000; 4] },
                    },
                    Event::Edge { from: FunctionId(1), to: FunctionId(2), count: 4 },
                    Event::Census {
                        ty: TypeId(1),
                        live: window,
                        bytes: window * 100,
                        sizes: Dist::empty(),
                    },
                ],
            });
        }
        drop(process);
        state
    }

    #[test]
    fn the_snapshot_names_every_field_the_web_client_reads() {
        let state = process_with_traffic();
        let json = serde_json::to_value(snapshot(&state.read().unwrap(), 1_700_000_001_000)).unwrap();

        for key in ["process", "taken_unix_ms", "rate_window_ms", "fine_window_ms", "functions", "types", "edges", "windows"] {
            assert!(json.get(key).is_some(), "snapshot lost the `{key}` field");
        }
        for key in ["id", "app", "pid", "source", "started_unix_ms", "ended_unix_ms", "connected", "window_ms", "counts"] {
            assert!(json["process"].get(key).is_some(), "process summary lost the `{key}` field");
        }
        for key in ["id", "name", "calls_total", "ns_total", "calls_per_sec", "ns_per_sec", "ns_buckets"] {
            assert!(json["functions"][0].get(key).is_some(), "function lost the `{key}` field");
        }
        for key in ["live", "bytes", "at_t_ns", "size_buckets"] {
            assert!(json["types"][0]["census"].get(key).is_some(), "census lost the `{key}` field");
        }
        for key in ["from", "to", "calls_total"] {
            assert!(json["edges"][0].get(key).is_some(), "edge lost the `{key}` field");
        }
        for key in ["index", "start_t_ns", "end_t_ns", "calls", "edges", "census"] {
            assert!(json["windows"][0].get(key).is_some(), "window lost the `{key}` field");
        }
    }

    #[test]
    fn the_snapshot_reports_a_census_as_the_latest_reading_and_calls_as_a_running_total() {
        let state = process_with_traffic();
        let snapshot = snapshot(&state.read().unwrap(), 1_700_000_001_000);

        let decode = snapshot.functions.iter().find(|f| f.id == 1).expect("decode is listed");
        assert_eq!(decode.calls_total, 40, "ten windows of four calls accumulate");
        assert_eq!(decode.ns_total, 40_000_000);

        let thumbnail = snapshot.types.iter().find(|t| t.id == 1).expect("Thumbnail is listed");
        let census = thumbnail.census.as_ref().expect("a reading arrived");
        assert_eq!(census.live, 10, "the last reading, not 1 + 2 + ... + 10");
        assert_eq!(census.bytes, 1_000);
    }

    #[test]
    fn the_rate_is_averaged_over_the_documented_window() {
        let state = process_with_traffic();
        let snapshot = snapshot(&state.read().unwrap(), 1_700_000_001_000);
        let decode = snapshot.functions.iter().find(|f| f.id == 1).expect("decode is listed");
        assert_eq!(snapshot.rate_window_ms, 1_000);
        assert_eq!(decode.calls_per_sec, 40.0, "four calls per 100ms window is forty a second");
    }

    #[test]
    fn a_bucket_carries_the_value_range_so_a_client_need_not_port_the_bucketing() {
        let state = process_with_traffic();
        let snapshot = snapshot(&state.read().unwrap(), 1_700_000_001_000);
        let decode = snapshot.functions.iter().find(|f| f.id == 1).expect("decode is listed");
        let bucket = decode.ns_buckets.first().expect("durations were recorded");
        assert!(bucket.lo <= 1_000_000 && 1_000_000 <= bucket.hi, "the sample is inside its own range");
        assert_eq!(bucket.count, 40);
    }
}
