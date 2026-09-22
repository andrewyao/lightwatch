//! What the daemon remembers about every process that has ever connected.
//!
//! One [`ProcessState`] per process, behind its own lock, so a busy emitter
//! never blocks another one or an HTTP reader. A process that disconnects keeps
//! its state and is marked ended, because the last thing a program did before
//! it died is usually the thing you opened the profiler for.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use lightwatch_proto::{
    bucket_range, Dist, Event, Frame, FunctionId, Hello, Location, PathId, Register, TypeId,
};
use tokio::sync::broadcast;

use crate::quantity::{Absolute, Cumulative};
use crate::ring::{CensusReading, Ring, Window};

/// How many closed windows a slow websocket client may fall behind before it
/// starts missing them.
const UPDATE_BACKLOG: usize = 256;

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Identifies one stream from one run of one program. `started_unix_ms` keeps a
/// recycled pid from colliding with the process that used to own it, and lets a
/// stream that reconnects resume its own history.
///
/// `source` is in the id because two emitters watching one OS process agree on
/// the pid and can agree on the start time as well. The probe reads
/// `SystemTime::now()` as it starts; a bridge outside the target estimates
/// `now - elapsed`, and when both happen at the top of `main` the two land on
/// the same millisecond. Without the source they share one `ProcessState`, and
/// two independent `seq` counters folded into one ring inflate the call totals
/// by orders of magnitude while reporting the gap as missed frames. Pairing the
/// two back together is [`crate::session`]'s job, at read time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessId(String);

impl ProcessId {
    pub fn of(pid: u32, started_unix_ms: u64, source: &str) -> Self {
        ProcessId(format!("{pid}-{started_unix_ms}-{source}"))
    }

    pub fn of_hello(hello: &Hello) -> Self {
        ProcessId::of(hello.pid, hello.started_unix_ms, &hello.source)
    }
}

impl From<&str> for ProcessId {
    fn from(raw: &str) -> Self {
        ProcessId(raw.to_string())
    }
}

impl std::fmt::Display for ProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Everything the boundary rejected or noticed, per process.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub frames: u64,
    pub events: u64,
    /// Events naming a function or type that was never registered.
    pub unknown_id: u64,
    /// Lines that did not parse, plus lines that parsed into something a
    /// mid-stream frame may not be.
    pub malformed_lines: u64,
    /// Windows the emitter itself admits it dropped, counted from `seq` gaps.
    pub missed_frames: u64,
    /// Frames whose `t_ns` fell before the oldest window the fine ring holds.
    pub late_frames: u64,
}

#[derive(Debug, Clone, Default)]
pub struct FunctionState {
    pub name: String,
    pub module: Option<String>,
    pub location: Option<Location>,
    pub calls: Cumulative,
    pub ns_total: Cumulative,
    /// Time spent in this function and outside any measured callee, summed
    /// over every context it was reached through. Unlike `ns_total`, adding
    /// this across every function double-counts nothing.
    pub self_ns_total: Cumulative,
    /// Lifetime duration histogram, bucket index to sample count.
    pub ns_buckets: BTreeMap<u16, u64>,
}

/// One node of a process's call tree: `func`, reached through `parent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathNode {
    pub parent: PathId,
    pub func: FunctionId,
}

/// One calling context's lifetime totals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StackTotals {
    pub calls: Cumulative,
    pub self_ns: Cumulative,
}

#[derive(Debug, Clone, Default)]
pub struct TypeState {
    pub name: String,
    pub location: Option<Location>,
    /// The most recent reading. Never a running total.
    pub latest: Option<CensusReading>,
}

/// What a websocket subscriber receives.
#[derive(Debug)]
pub enum Update {
    /// A fine window that can no longer change.
    WindowClosed(Box<Window>),
    ProcessEnded { ended_unix_ms: u64 },
}

pub struct ProcessState {
    pub id: ProcessId,
    pub app: String,
    pub pid: u32,
    pub source: String,
    pub started_unix_ms: u64,
    pub ended_unix_ms: Option<u64>,
    pub window_ms: u32,
    pub last_seq: Option<u64>,
    pub last_t_ns: u64,
    pub last_frame_unix_ms: Option<u64>,
    pub counts: Counts,
    pub functions: BTreeMap<FunctionId, FunctionState>,
    pub types: BTreeMap<TypeId, TypeState>,
    /// Kept for the whole process lifetime, never windowed. A call graph that
    /// forgets an edge the moment it stops firing flickers and is unreadable.
    pub edges: BTreeMap<(FunctionId, FunctionId), Cumulative>,
    /// The call tree, kept for the whole process lifetime like `edges`. A
    /// flame graph that forgot a context the moment it went quiet would
    /// change shape under the reader between one window and the next.
    pub paths: BTreeMap<PathId, PathNode>,
    pub path_totals: BTreeMap<PathId, StackTotals>,
    pub fine: Ring,
    pub coarse: Ring,
    open_fine_index: Option<u64>,
    live_connections: u32,
    updates: broadcast::Sender<Arc<Update>>,
}

impl ProcessState {
    fn new(id: ProcessId, hello: &Hello) -> Self {
        ProcessState {
            id,
            app: hello.app.clone(),
            pid: hello.pid,
            source: hello.source.clone(),
            started_unix_ms: hello.started_unix_ms,
            ended_unix_ms: None,
            window_ms: hello.window_ms,
            last_seq: None,
            last_t_ns: 0,
            last_frame_unix_ms: None,
            counts: Counts::default(),
            functions: BTreeMap::new(),
            types: BTreeMap::new(),
            edges: BTreeMap::new(),
            paths: BTreeMap::new(),
            path_totals: BTreeMap::new(),
            fine: Ring::fine(),
            coarse: Ring::coarse(),
            open_fine_index: None,
            live_connections: 0,
            updates: broadcast::channel(UPDATE_BACKLOG).0,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.live_connections > 0
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Update>> {
        self.updates.subscribe()
    }

    pub fn note_malformed_line(&mut self) {
        self.counts.malformed_lines += 1;
    }

    /// Folds one frame in. Registers land before events, so a frame may name an
    /// id it introduces in the same breath.
    pub fn apply_frame(&mut self, frame: &Frame) {
        self.counts.frames += 1;
        if let Some(previous) = self.last_seq {
            if frame.seq > previous + 1 {
                self.counts.missed_frames += frame.seq - previous - 1;
            }
        }
        self.last_seq = Some(frame.seq);
        self.last_t_ns = self.last_t_ns.max(frame.t_ns);
        self.last_frame_unix_ms = Some(now_unix_ms());

        for register in &frame.registers {
            self.register(register);
        }

        let closed = self.close_open_window_before(frame.t_ns);

        let applied = self.accumulate_totals(&frame.events, frame.t_ns);
        self.fold_into_rings(&applied, frame.t_ns);

        if let Some(window) = closed {
            let _ = self.updates.send(Arc::new(Update::WindowClosed(Box::new(window))));
        }
    }

    fn register(&mut self, register: &Register) {
        match register {
            // A re-registration carries new metadata but not a new history, so
            // the counters behind the id survive it.
            Register::Function { id, name, module, location } => {
                let entry = self.functions.entry(*id).or_default();
                entry.name = name.clone();
                entry.module = module.clone();
                entry.location = location.clone();
            }
            Register::Type { id, name, location } => {
                let entry = self.types.entry(*id).or_default();
                entry.name = name.clone();
                entry.location = location.clone();
            }
            // A context is only meaningful once both ends of it are known, so
            // a node naming an unregistered function, or hanging off a parent
            // this stream never introduced, is dropped rather than stored as
            // a root it is not.
            Register::Path { id, parent, func } => {
                if id.0 == 0 || !self.functions.contains_key(func) {
                    self.counts.unknown_id += 1;
                    return;
                }
                if parent.0 != 0 && !self.paths.contains_key(parent) {
                    self.counts.unknown_id += 1;
                    return;
                }
                self.paths.insert(*id, PathNode { parent: *parent, func: *func });
            }
        }
    }

    /// The function that reaches `node`, or `None` when `node` is a root and
    /// nothing measured called it.
    fn caller_of(&self, node: PathNode) -> Option<FunctionId> {
        if node.parent.0 == 0 {
            return None;
        }
        self.paths.get(&node.parent).map(|parent| parent.func)
    }

    /// The fine window that this frame's arrival puts out of reach, if any.
    fn close_open_window_before(&mut self, t_ns: u64) -> Option<Window> {
        let index = self.fine.index_of(t_ns);
        match self.open_fine_index {
            Some(open) if index > open => {
                self.open_fine_index = Some(index);
                self.fine.get(open).cloned()
            }
            Some(_) => None,
            None => {
                self.open_fine_index = Some(index);
                None
            }
        }
    }

    /// Validates every event against the registered ids and updates the
    /// lifetime totals, returning what survived for the rings to fold in.
    fn accumulate_totals(&mut self, events: &[Event], t_ns: u64) -> Vec<Applied> {
        let mut applied = Vec::with_capacity(events.len());
        for event in events {
            self.counts.events += 1;
            match event {
                Event::Calls { func, count, ns } => {
                    let buckets = ns.to_buckets();
                    let elapsed = duration_total(ns, &buckets);
                    let Some(state) = self.functions.get_mut(func) else {
                        self.counts.unknown_id += 1;
                        continue;
                    };
                    state.calls.accumulate(*count);
                    state.ns_total.accumulate(elapsed);
                    for (bucket, samples) in &buckets {
                        *state.ns_buckets.entry(*bucket).or_insert(0) += *samples as u64;
                    }
                    applied.push(Applied::Calls { func: *func, calls: *count, ns: elapsed });
                }
                // Superseded by the call tree, which says the same thing and
                // also says through which chain of callers. Ignored rather
                // than counted against the feed: an emitter that still sends
                // both is not malformed, it is a version behind.
                Event::Edge { .. } => {}
                Event::Stack { path, count, self_ns } => {
                    let Some(node) = self.paths.get(path).copied() else {
                        self.counts.unknown_id += 1;
                        continue;
                    };
                    let totals = self.path_totals.entry(*path).or_default();
                    totals.calls.accumulate(*count);
                    totals.self_ns.accumulate(*self_ns);
                    if let Some(state) = self.functions.get_mut(&node.func) {
                        state.self_ns_total.accumulate(*self_ns);
                    }
                    applied.push(Applied::Stack {
                        path: *path,
                        calls: *count,
                        self_ns: *self_ns,
                    });

                    // The edge this context stands for. A context knows both
                    // ends of it, so the graph is a reading of the tree
                    // rather than a second set of counters that could
                    // disagree with it.
                    if let Some(caller) = self.caller_of(node) {
                        self.edges.entry((caller, node.func)).or_default().accumulate(*count);
                        applied.push(Applied::Edge {
                            from: caller,
                            to: node.func,
                            calls: *count,
                        });
                    }
                }
                Event::Census { ty, live, bytes, sizes } => {
                    let reading = CensusReading {
                        live: Absolute::reading(*live),
                        bytes: Absolute::reading(*bytes),
                        size_buckets: sizes.to_buckets(),
                        at_t_ns: t_ns,
                    };
                    let Some(state) = self.types.get_mut(ty) else {
                        self.counts.unknown_id += 1;
                        continue;
                    };
                    let newer_already_held =
                        state.latest.as_ref().is_some_and(|held| held.at_t_ns > reading.at_t_ns);
                    if !newer_already_held {
                        state.latest = Some(reading.clone());
                    }
                    applied.push(Applied::Census { ty: *ty, reading });
                }
            }
        }
        applied
    }

    fn fold_into_rings(&mut self, applied: &[Applied], t_ns: u64) {
        if applied.is_empty() {
            return;
        }
        let coarse = self.coarse.window_for(t_ns);
        fold(coarse, applied);
        match self.fine.window_for(t_ns) {
            Some(window) => fold(Some(window), applied),
            None => self.counts.late_frames += 1,
        }
    }

    /// Seals the window still open, so the last thing a dying process did is
    /// pushed to a watching UI rather than stranded.
    fn flush_open_window(&mut self) {
        if let Some(open) = self.open_fine_index.take() {
            if let Some(window) = self.fine.get(open) {
                if !window.is_empty() {
                    let sealed = window.clone();
                    let _ = self.updates.send(Arc::new(Update::WindowClosed(Box::new(sealed))));
                }
            }
        }
    }
}

enum Applied {
    Calls { func: FunctionId, calls: u64, ns: u64 },
    Edge { from: FunctionId, to: FunctionId, calls: u64 },
    Stack { path: PathId, calls: u64, self_ns: u64 },
    Census { ty: TypeId, reading: CensusReading },
}

fn fold(window: Option<&mut Window>, applied: &[Applied]) {
    let Some(window) = window else { return };
    for item in applied {
        match item {
            Applied::Calls { func, calls, ns } => window.add_calls(*func, *calls, *ns),
            Applied::Edge { from, to, calls } => window.add_edge(*from, *to, *calls),
            Applied::Stack { path, calls, self_ns } => window.add_stack(*path, *calls, *self_ns),
            Applied::Census { ty, reading } => window.record_census(*ty, reading.clone()),
        }
    }
}

/// Total nanoseconds a distribution stands for. Exact when the client sent raw
/// samples; a bucket-midpoint estimate, within the 6.25% the bucketing
/// guarantees, when it sent a histogram.
fn duration_total(dist: &Dist, buckets: &[(u16, u32)]) -> u64 {
    if let Dist::Raw { v } = dist {
        return v.iter().fold(0u64, |total, sample| total.saturating_add(*sample));
    }
    buckets.iter().fold(0u64, |total, (index, samples)| {
        let (lo, hi) = bucket_range(*index);
        let midpoint = lo + (hi - lo) / 2;
        total.saturating_add(midpoint.saturating_mul(*samples as u64))
    })
}

/// Every process the daemon has seen since it started.
#[derive(Default)]
pub struct Registry {
    processes: RwLock<BTreeMap<ProcessId, Arc<RwLock<ProcessState>>>>,
}

impl Registry {
    pub fn new() -> Self {
        Registry::default()
    }

    /// Admits a connection. A process that reconnects resumes the state it
    /// left behind rather than starting a second, half-empty history.
    pub fn connect(&self, hello: &Hello) -> Arc<RwLock<ProcessState>> {
        let id = ProcessId::of_hello(hello);
        let state = {
            let mut processes = self.processes.write().expect("registry lock");
            processes
                .entry(id.clone())
                .or_insert_with(|| Arc::new(RwLock::new(ProcessState::new(id.clone(), hello))))
                .clone()
        };
        {
            let mut process = state.write().expect("process lock");
            process.app = hello.app.clone();
            process.source = hello.source.clone();
            process.window_ms = hello.window_ms;
            process.ended_unix_ms = None;
            process.live_connections += 1;
        }
        state
    }

    pub fn disconnect(&self, id: &ProcessId) {
        let Some(state) = self.get(id) else { return };
        let mut process = state.write().expect("process lock");
        process.live_connections = process.live_connections.saturating_sub(1);
        if process.live_connections == 0 {
            let ended_unix_ms = now_unix_ms();
            process.ended_unix_ms = Some(ended_unix_ms);
            process.flush_open_window();
            let _ = process.updates.send(Arc::new(Update::ProcessEnded { ended_unix_ms }));
        }
    }

    pub fn get(&self, id: &ProcessId) -> Option<Arc<RwLock<ProcessState>>> {
        self.processes.read().expect("registry lock").get(id).cloned()
    }

    pub fn all(&self) -> Vec<Arc<RwLock<ProcessState>>> {
        self.processes.read().expect("registry lock").values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.processes.read().expect("registry lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightwatch_proto::SCHEMA_VERSION;
    use std::collections::BTreeSet;

    fn hello() -> Hello {
        Hello::new("demo", "test", 4242, 1_700_000_000_000)
    }

    fn registered_frame() -> Frame {
        Frame {
            seq: 0,
            t_ns: 0,
            registers: vec![
                Register::Function {
                    id: FunctionId(1),
                    name: "decode".into(),
                    module: Some("demo".into()),
                    location: None,
                },
                Register::Function { id: FunctionId(2), name: "resize".into(), module: None, location: None },
                Register::Type { id: TypeId(1), name: "Thumbnail".into(), location: None },
                Register::Path { id: PathId(1), parent: PathId(0), func: FunctionId(1) },
                Register::Path { id: PathId(2), parent: PathId(1), func: FunctionId(2) },
            ],
            events: vec![],
        }
    }

    fn census_frame(seq: u64, t_ns: u64, live: u64, bytes: u64) -> Frame {
        Frame {
            seq,
            t_ns,
            registers: vec![],
            events: vec![Event::Census {
                ty: TypeId(1),
                live,
                bytes,
                sizes: Dist::Raw { v: vec![bytes / live.max(1); live as usize] },
            }],
        }
    }

    fn calls_frame(seq: u64, t_ns: u64, count: u64) -> Frame {
        Frame {
            seq,
            t_ns,
            registers: vec![],
            events: vec![
                Event::Calls { func: FunctionId(1), count, ns: Dist::Raw { v: vec![1_000; count as usize] } },
                Event::Edge { from: FunctionId(1), to: FunctionId(2), count },
                Event::Stack { path: PathId(2), count, self_ns: count * 600 },
                Event::Stack { path: PathId(1), count, self_ns: count * 400 },
            ],
        }
    }

    fn connected() -> Arc<RwLock<ProcessState>> {
        let registry = Registry::new();
        let state = registry.connect(&hello());
        state.write().unwrap().apply_frame(&registered_frame());
        state
    }

    #[test]
    fn a_calling_context_accumulates_its_self_time_as_a_delta() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&calls_frame(1, 100_000_000, 10));
        process.apply_frame(&calls_frame(2, 200_000_000, 10));

        assert_eq!(process.path_totals[&PathId(2)].calls.get(), 20);
        assert_eq!(process.path_totals[&PathId(2)].self_ns.get(), 12_000);
        assert_eq!(
            process.functions[&FunctionId(2)].self_ns_total.get(),
            12_000,
            "a function's self time is the sum over every context it was reached through"
        );
    }

    #[test]
    fn self_time_over_the_tree_never_exceeds_the_inclusive_time_reported_for_the_root() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&calls_frame(1, 100_000_000, 10));

        let over_the_tree: u64 = process.path_totals.values().map(|t| t.self_ns.get()).sum();
        assert_eq!(over_the_tree, 10_000);
        assert_eq!(
            process.functions[&FunctionId(1)].ns_total.get(),
            10_000,
            "the tree's self time and the root's inclusive time are the same nanoseconds"
        );
    }

    #[test]
    fn a_stack_naming_an_unregistered_context_is_counted_and_dropped() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&Frame {
            seq: 1,
            t_ns: 100_000_000,
            registers: vec![],
            events: vec![Event::Stack { path: PathId(98), count: 3, self_ns: 500 }],
        });

        assert_eq!(process.counts.unknown_id, 1);
        assert!(process.path_totals.is_empty());
    }

    #[test]
    fn a_context_hanging_off_a_parent_this_stream_never_introduced_is_refused() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&Frame {
            seq: 1,
            t_ns: 100_000_000,
            registers: vec![
                // Parent 77 was never registered, and function 99 never was
                // either. Neither may become a root by default.
                Register::Path { id: PathId(3), parent: PathId(77), func: FunctionId(1) },
                Register::Path { id: PathId(4), parent: PathId(0), func: FunctionId(99) },
            ],
            events: vec![],
        });

        assert_eq!(process.counts.unknown_id, 2);
        assert!(!process.paths.contains_key(&PathId(3)));
        assert!(!process.paths.contains_key(&PathId(4)));
    }

    #[test]
    fn the_coarse_ring_rolls_stacks_up_the_way_it_rolls_calls_up() {
        let state = connected();
        let mut process = state.write().unwrap();
        for step in 0..10u64 {
            process.apply_frame(&calls_frame(step + 1, step * 100_000_000, 2));
        }

        let coarse = process.coarse.get(0).expect("one coarse window covers ten fine ones");
        assert_eq!(coarse.stacks[&PathId(2)].calls.get(), 20);
        assert_eq!(coarse.stacks[&PathId(2)].self_ns.get(), 12_000);
        let fine: u64 = process.fine.iter().filter_map(|w| w.stacks.get(&PathId(2))).map(|d| d.self_ns.get()).sum();
        assert_eq!(fine, coarse.stacks[&PathId(2)].self_ns.get());
    }

    #[test]
    fn a_census_reading_replaces_the_previous_one_instead_of_accumulating() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&census_frame(1, 100_000_000, 3, 300));
        process.apply_frame(&census_frame(2, 200_000_000, 5, 500));
        process.apply_frame(&census_frame(3, 300_000_000, 2, 200));

        let latest = process.types[&TypeId(1)].latest.as_ref().expect("a census was recorded");
        assert_eq!(latest.live.get(), 2, "live objects is the last reading, not 3 + 5 + 2");
        assert_eq!(latest.bytes.get(), 200, "footprint is the last reading, not 300 + 500 + 200");

        let per_window: Vec<u64> = process
            .fine
            .iter()
            .filter_map(|w| w.census.get(&TypeId(1)))
            .map(|c| c.live.get())
            .collect();
        assert_eq!(per_window, vec![3, 5, 2], "each window keeps its own reading");
    }

    #[test]
    fn call_and_edge_deltas_accumulate_across_windows() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&calls_frame(1, 100_000_000, 4));
        process.apply_frame(&calls_frame(2, 200_000_000, 6));
        process.apply_frame(&calls_frame(3, 300_000_000, 10));

        assert_eq!(process.functions[&FunctionId(1)].calls.get(), 20);
        assert_eq!(process.functions[&FunctionId(1)].ns_total.get(), 20_000);
        assert_eq!(process.edges[&(FunctionId(1), FunctionId(2))].get(), 20);

        let window = process.fine.get(2).expect("the second window is retained");
        assert_eq!(window.calls[&FunctionId(1)].calls.get(), 6, "a window holds its own delta only");
    }

    #[test]
    fn two_frames_inside_one_window_accumulate_into_that_window() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&calls_frame(1, 10_000_000, 4));
        process.apply_frame(&calls_frame(2, 50_000_000, 6));
        assert_eq!(process.fine.len(), 1);
        assert_eq!(process.fine.get(0).unwrap().calls[&FunctionId(1)].calls.get(), 10);
    }

    #[test]
    fn an_event_naming_an_unregistered_id_is_counted_and_dropped() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&Frame {
            seq: 1,
            t_ns: 100_000_000,
            registers: vec![],
            events: vec![
                Event::Calls { func: FunctionId(99), count: 5, ns: Dist::empty() },
                Event::Stack { path: PathId(98), count: 5, self_ns: 500 },
                Event::Census { ty: TypeId(97), live: 1, bytes: 1, sizes: Dist::empty() },
            ],
        });

        assert_eq!(process.counts.unknown_id, 3);
        assert_eq!(process.counts.events, 3);
        assert!(!process.functions.contains_key(&FunctionId(99)), "an unknown id is never invented");
        assert!(process.edges.is_empty());
        assert!(process.path_totals.is_empty());
        assert!(process.fine.get(1).is_none_or(|w| w.is_empty()), "a dropped event reaches no window");
    }

    #[test]
    fn the_edge_map_is_read_off_the_call_tree_and_says_what_an_edge_event_used_to() {
        // The demo app's documented graph, as contexts:
        //   a -> b -> c, pipeline -> {Decode::run -> c, Encode::run},
        //   render -> paint, descend -> descend.
        let names = ["a", "b", "c", "pipeline", "Decode::run", "Encode::run", "render", "paint", "descend"];
        let tree = [
            (1u32, 0u32, 1u32), // a
            (2, 1, 2),          // a > b
            (3, 2, 3),          // a > b > c
            (4, 0, 4),          // pipeline
            (5, 4, 5),          // pipeline > Decode::run
            (6, 5, 3),          // pipeline > Decode::run > c
            (7, 4, 6),          // pipeline > Encode::run
            (8, 0, 7),          // render
            (9, 8, 8),          // render > paint (blit is not measured)
            (10, 0, 9),         // descend
            (11, 10, 9),        // descend > descend, one node however deep
        ];

        let registry = Registry::new();
        let state = registry.connect(&hello());
        let mut process = state.write().unwrap();
        process.apply_frame(&Frame {
            seq: 0,
            t_ns: 0,
            registers: names
                .iter()
                .enumerate()
                .map(|(index, name)| Register::Function {
                    id: FunctionId(index as u32 + 1),
                    name: (*name).into(),
                    module: None,
                    location: None,
                })
                .chain(tree.iter().map(|(id, parent, func)| Register::Path {
                    id: PathId(*id),
                    parent: PathId(*parent),
                    func: FunctionId(*func),
                }))
                .collect(),
            events: tree
                .iter()
                .map(|(id, _, _)| Event::Stack { path: PathId(*id), count: 3, self_ns: 1_000 })
                .collect(),
        });

        let named: BTreeSet<(&str, &str)> = process
            .edges
            .keys()
            .map(|(from, to)| {
                (names[from.0 as usize - 1], names[to.0 as usize - 1])
            })
            .collect();
        assert_eq!(
            named,
            BTreeSet::from([
                ("a", "b"),
                ("b", "c"),
                ("pipeline", "Decode::run"),
                ("pipeline", "Encode::run"),
                ("Decode::run", "c"),
                ("render", "paint"),
                ("descend", "descend"),
            ])
        );
        assert_eq!(
            process.edges[&(FunctionId(1), FunctionId(2))].get(),
            3,
            "an edge's weight is how often its context was entered"
        );
        assert!(
            !named.iter().any(|(from, _)| *from == "blit"),
            "an unmeasured frame has no context, so it can have no edge"
        );
    }

    #[test]
    fn a_gap_in_the_sequence_is_reported_as_frames_the_emitter_dropped() {
        let state = connected();
        let mut process = state.write().unwrap();
        process.apply_frame(&calls_frame(1, 100_000_000, 1));
        process.apply_frame(&calls_frame(5, 500_000_000, 1));
        assert_eq!(process.counts.missed_frames, 3);
    }

    #[test]
    fn the_coarse_ring_rolls_ten_fine_windows_into_one() {
        let state = connected();
        let mut process = state.write().unwrap();
        for window in 1..=12u64 {
            process.apply_frame(&calls_frame(window, window * 100_000_000, 2));
        }
        assert_eq!(process.coarse.get(0).unwrap().calls[&FunctionId(1)].calls.get(), 18);
        assert_eq!(process.coarse.get(1).unwrap().calls[&FunctionId(1)].calls.get(), 6);
    }

    #[test]
    fn a_process_that_disconnects_stays_inspectable_and_is_marked_ended() {
        let registry = Registry::new();
        let state = registry.connect(&hello());
        state.write().unwrap().apply_frame(&registered_frame());
        state.write().unwrap().apply_frame(&calls_frame(1, 100_000_000, 7));
        registry.disconnect(&ProcessId::of(4242, 1_700_000_000_000, "test"));

        let held = registry.get(&ProcessId::of(4242, 1_700_000_000_000, "test")).expect("still listed");
        let process = held.read().unwrap();
        assert!(process.ended_unix_ms.is_some());
        assert!(!process.is_connected());
        assert_eq!(process.functions[&FunctionId(1)].calls.get(), 7, "its last state survives");
    }

    #[test]
    fn a_process_that_reconnects_resumes_its_own_history() {
        let registry = Registry::new();
        let state = registry.connect(&hello());
        state.write().unwrap().apply_frame(&registered_frame());
        state.write().unwrap().apply_frame(&calls_frame(1, 100_000_000, 7));
        registry.disconnect(&ProcessId::of(4242, 1_700_000_000_000, "test"));

        let again = registry.connect(&hello());
        assert_eq!(registry.len(), 1, "a reconnect is not a second process");
        let process = again.read().unwrap();
        assert!(process.ended_unix_ms.is_none());
        assert_eq!(process.functions[&FunctionId(1)].calls.get(), 7);
    }

    #[test]
    fn two_emitters_that_agree_on_the_start_millisecond_still_get_a_history_each() {
        // Reproduced live on lightphotos pid 69185: the probe reads
        // `SystemTime::now()` at the top of `main` and a bridge outside the
        // target estimates `now - elapsed` from the same instant, so both
        // announced 1790021787941 and the registry handed them one state. The
        // bridge's calls then folded into the probe's ring behind the probe's
        // `seq`, and `working_thumb_keys` read 6,950/s against a true 125/s.
        let registry = Registry::new();
        let started = 1_790_021_787_941;
        let probe = registry.connect(&Hello::new("lightphotos", "lightwatch-probe", 69185, started));
        let bridge =
            registry.connect(&Hello::new("lightphotos", "lightwatch-hotpath", 69185, started));
        assert_eq!(registry.len(), 2, "one OS process watched twice is two streams");
        assert!(!Arc::ptr_eq(&probe, &bridge), "and they must not share a ProcessState");

        let type_only = Frame {
            seq: 399,
            t_ns: 0,
            registers: vec![Register::Type { id: TypeId(1), name: "DecodedImage".into(), location: None }],
            events: vec![],
        };
        let functions_only = Frame {
            seq: 0,
            t_ns: 0,
            registers: vec![
                Register::Function {
                    id: FunctionId(1),
                    name: "working_thumb_keys".into(),
                    module: None,
                    location: None,
                },
                Register::Function { id: FunctionId(2), name: "sync".into(), module: None, location: None },
            ],
            events: vec![],
        };
        probe.write().unwrap().apply_frame(&type_only);
        bridge.write().unwrap().apply_frame(&functions_only);

        // The probe has been emitting since the program started and is deep
        // into its own sequence; the bridge attached later and starts at zero.
        for step in 1..=5u64 {
            let t_ns = step * 100_000_000;
            probe.write().unwrap().apply_frame(&census_frame(399 + step, t_ns, 21, 15_024_107));
            bridge.write().unwrap().apply_frame(&calls_frame(step, t_ns, 125));
        }

        let probe = probe.read().unwrap();
        let bridge = bridge.read().unwrap();
        assert_eq!(probe.source, "lightwatch-probe", "neither hello overwrites the other's");
        assert_eq!(bridge.source, "lightwatch-hotpath");
        assert_eq!(probe.counts.missed_frames, 0, "seq 400 is not a gap in a sequence that starts at 0");
        assert_eq!(bridge.counts.missed_frames, 0);
        assert_eq!(
            bridge.functions[&FunctionId(1)].calls.get(),
            625,
            "the bridge's own calls, not two emitters' frames folded together"
        );
        assert_eq!(probe.types[&TypeId(1)].latest.as_ref().expect("a census").live.get(), 21);
        assert!(probe.functions.is_empty(), "the probe registered no function and holds none");
        assert!(bridge.types.is_empty(), "the bridge registered no type and holds none");
    }

    #[test]
    fn a_recycled_pid_is_a_different_process() {
        let registry = Registry::new();
        registry.connect(&Hello::new("demo", "test", 4242, 1_700_000_000_000));
        registry.connect(&Hello::new("demo", "test", 4242, 1_700_000_009_999));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn a_bucketed_duration_lands_within_the_bound_the_protocol_promises() {
        let state = connected();
        let mut process = state.write().unwrap();
        let exact: u64 = [5_000_000u64; 8].iter().sum();
        process.apply_frame(&Frame {
            seq: 1,
            t_ns: 100_000_000,
            registers: vec![],
            events: vec![Event::Calls {
                func: FunctionId(1),
                count: 8,
                ns: Dist::Buckets { b: Dist::Raw { v: vec![5_000_000; 8] }.to_buckets() },
            }],
        });
        let estimated = process.functions[&FunctionId(1)].ns_total.get();
        let error = estimated.abs_diff(exact) as f64 / exact as f64;
        assert!(error <= 0.0625, "estimate {estimated} strayed {error} from {exact}");
    }

    #[test]
    fn a_closed_window_is_pushed_to_a_subscriber_once_it_can_no_longer_change() {
        let state = connected();
        let mut receiver = state.read().unwrap().subscribe();
        {
            let mut process = state.write().unwrap();
            process.apply_frame(&calls_frame(1, 100_000_000, 3));
            process.apply_frame(&calls_frame(2, 200_000_000, 4));
        }
        let update = receiver.try_recv().expect("one window closed");
        match &*update {
            Update::WindowClosed(window) => {
                assert_eq!(window.index, 1);
                assert_eq!(window.calls[&FunctionId(1)].calls.get(), 3);
            }
            other => panic!("expected a closed window, got {other:?}"),
        }
        assert!(receiver.try_recv().is_err(), "the window still open is not pushed");
    }

    #[test]
    fn the_schema_the_daemon_speaks_is_the_one_the_protocol_declares() {
        assert_eq!(hello().schema, SCHEMA_VERSION);
    }
}
