//! Who called whom, and for how long.
//!
//! Each thread keeps its own activation stack and its own accumulator. The
//! stack is plain thread-local data the emit thread never sees. The
//! accumulator sits behind a mutex the owning thread holds for the few
//! instructions it takes to bump two counters, and that the emit thread takes
//! once per window to swap the maps out. There is no cross-thread contention
//! on the hot path: two threads never touch the same accumulator.
//!
//! Only edges where both ends carry `#[measure]` are visible. An
//! uninstrumented frame between two measured functions collapses into a direct
//! edge from the outer one to the inner one. That is the intended reading of
//! the graph, not a defect to work around.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use lightwatch_proto::dist::{bucket_of, Dist, MAX_RAW_SAMPLES};
use lightwatch_proto::FunctionId;

use crate::hashing::IntMap;

/// One `#[measure]`d function, as a `static` at its own definition site.
pub struct Site {
    pub(crate) name: &'static str,
    pub(crate) module: &'static str,
    pub(crate) file: &'static str,
    pub(crate) line: u32,
    id: std::sync::atomic::AtomicU32,
}

impl Site {
    pub const fn new(
        name: &'static str,
        module: &'static str,
        file: &'static str,
        line: u32,
    ) -> Self {
        Site {
            name,
            module,
            file,
            line,
            id: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub(crate) fn interned_id(&self) -> Option<FunctionId> {
        match self.id.load(std::sync::atomic::Ordering::Relaxed) {
            0 => None,
            assigned => Some(FunctionId(assigned)),
        }
    }

    pub(crate) fn publish_id(&self, id: FunctionId) {
        self.id.store(id.0, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn qualified(&self) -> String {
        format!("{}::{}", self.module, self.name)
    }

    fn id(&'static self) -> u32 {
        self.interned_id()
            .unwrap_or_else(|| crate::registry::intern_function(self))
            .0
    }

    /// Opens an activation. The returned guard closes it when it drops, which
    /// includes a panic unwinding past it, so the stack stays balanced.
    pub fn enter(&'static self) -> Guard {
        let id = self.id();
        let opened = STATE
            .try_with(|state| {
                let mut state = state.borrow_mut();
                state.open(id);
            })
            .is_ok();
        Guard { opened, not_send: PhantomData }
    }
}

/// Closes the activation its `Site::enter` opened.
///
/// Deliberately `!Send`: an activation belongs to the thread that opened it,
/// and a guard that crossed threads would pop a stack it never pushed.
pub struct Guard {
    opened: bool,
    not_send: PhantomData<*const ()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.opened {
            return;
        }
        let _ = STATE.try_with(|state| state.borrow_mut().close());
    }
}

struct Activation {
    func: u32,
    /// Zero when this is the outermost measured frame on the thread.
    parent: u32,
    /// The calling context this activation runs in.
    path: u32,
    start: Instant,
    /// Inclusive time of the measured callees that closed inside this
    /// activation. Subtracted at close to leave self time.
    ///
    /// It lives here, on the activation, and not in a map keyed by function.
    /// A map looks equivalent and is shorter; under recursion it subtracts
    /// the same nanoseconds at every level, self time goes negative, and the
    /// saturating arithmetic below turns that into a silent zero.
    child_ns: u64,
    /// False when this function was already on the stack, so its time is
    /// already being counted by an enclosing activation.
    outermost: bool,
    /// Whether this activation is the one that opened its context. Every
    /// activation reports self time; only this one reports a call, so a
    /// recursive function counts once per outermost entry rather than once
    /// per level.
    counts_here: bool,
}

/// How deep the thread currently is inside one function.
struct Depth {
    count: u32,
    /// The context the outermost activation of this function runs in.
    path: u32,
    /// The context every recursive re-entry of this function runs in: a child
    /// of `path` naming the function again. Zero until the function recurses.
    recursion_path: u32,
    /// Set when the function called itself directly. Read when the outermost
    /// activation closes, which is the one place a self-edge is recorded.
    self_recursed: bool,
}

struct ThreadState {
    stack: Vec<Activation>,
    depth: IntMap<u32, Depth>,
    /// This thread's cache of the global context table, so the interner's
    /// lock is taken once per context the thread reaches rather than once per
    /// call. The ids it caches came from that table, so it cannot disagree
    /// with another thread about what a context is called.
    memo: IntMap<(u32, u32), u32>,
    slot: Arc<ThreadSlot>,
}

impl ThreadState {
    /// Interns `(parent_path, func)`, preferring this thread's cache.
    ///
    /// A full context table returns zero, and the activation then reports
    /// against its parent: the tree stops deepening instead of growing
    /// without bound.
    fn context(&mut self, parent_path: u32, func: u32) -> u32 {
        if let Some(known) = self.memo.get(&(parent_path, func)) {
            return *known;
        }
        let path = crate::registry::intern_path(parent_path, func);
        if path == 0 {
            return parent_path;
        }
        self.memo.insert((parent_path, func), path);
        path
    }

    fn open(&mut self, func: u32) {
        let parent = self.stack.last().map_or(0, |a| a.func);
        let parent_path = self.stack.last().map_or(0, |a| a.path);

        let held = self.depth.get(&func).map(|d| (d.count, d.path, d.recursion_path));
        let (path, outermost, counts_here) = match held {
            // Not on the stack: an ordinary context under whoever called it.
            None | Some((0, _, _)) => (self.context(parent_path, func), true, true),
            // Already on the stack, so this is recursion. Every level runs in
            // one shared context hanging off the function's own outermost
            // node, which is what keeps `descend(64)` from interning sixty-
            // four of them. Hanging it off the outermost node rather than off
            // the nearest one is also what bounds indirect recursion: the
            // cycle `f -> g -> f -> g` reaches four contexts and stops.
            Some((depth_count, outer_path, recursion_path)) => {
                let recursion_path = if recursion_path == 0 {
                    self.context(outer_path, func)
                } else {
                    recursion_path
                };
                if let Some(depth) = self.depth.get_mut(&func) {
                    depth.recursion_path = recursion_path;
                }
                (recursion_path, false, depth_count == 1)
            }
        };

        let depth = self.depth.entry(func).or_insert(Depth {
            count: 0,
            path,
            recursion_path: 0,
            self_recursed: false,
        });
        if outermost {
            depth.path = path;
            depth.recursion_path = 0;
        }
        depth.count += 1;
        if !outermost && parent == func {
            depth.self_recursed = true;
        }

        // Last, so interning a context the program has not reached before
        // never lands inside the span this activation is about to measure.
        self.stack.push(Activation {
            func,
            parent,
            path,
            start: Instant::now(),
            child_ns: 0,
            outermost,
            counts_here,
        });
    }

    fn close(&mut self) {
        // Read the clock before any bookkeeping, so none of it lands inside
        // the span being measured.
        let now = Instant::now();
        let Some(activation) = self.stack.pop() else {
            return;
        };
        let elapsed = now.saturating_duration_since(activation.start).as_nanos() as u64;

        // Charge this activation's inclusive time to whoever is still on the
        // stack, before any branch below can return early. A level whose time
        // never reaches its caller leaves that caller's self time inflated by
        // exactly this much, and the subtree stops summing to the whole.
        if let Some(caller) = self.stack.last_mut() {
            caller.child_ns = caller.child_ns.saturating_add(elapsed);
        }
        let self_ns = elapsed.saturating_sub(activation.child_ns);

        let mut self_recursed = false;
        if let Some(depth) = self.depth.get_mut(&activation.func) {
            depth.count -= 1;
            if depth.count == 0 {
                self_recursed = depth.self_recursed;
                self.depth.remove(&activation.func);
            }
        }

        // A direct self-call is a recursion level, not an edge of its own. The
        // self-edge is recorded once, below, when the outermost activation of
        // the function closes.
        let caller_edge = activation.parent != 0 && activation.parent != activation.func;
        let self_edge = activation.outermost && self_recursed;

        let mut accum = lock(&self.slot.accum);
        if caller_edge {
            *accum.edges.entry((activation.parent, activation.func)).or_insert(0) += 1;
        }
        if self_edge {
            *accum.edges.entry((activation.func, activation.func)).or_insert(0) += 1;
        }
        let stack = accum.stacks.entry(activation.path).or_default();
        stack.self_ns += self_ns;
        if activation.counts_here {
            stack.count += 1;
        }
        if activation.outermost {
            // Only the outermost activation contributes a duration, so nested
            // recursive time is counted once rather than once per level.
            let stat = accum.calls.entry(activation.func).or_default();
            stat.count += 1;
            stat.ns.add(elapsed);
        }
    }
}

/// The half of a thread's bookkeeping the emit thread is allowed to read.
pub(crate) struct ThreadSlot {
    accum: Mutex<Accum>,
}

static THREADS: Mutex<Vec<Arc<ThreadSlot>>> = Mutex::new(Vec::new());

thread_local! {
    static STATE: RefCell<ThreadState> = RefCell::new(ThreadState {
        stack: Vec::new(),
        depth: IntMap::default(),
        memo: IntMap::default(),
        slot: register_thread(),
    });
}

fn register_thread() -> Arc<ThreadSlot> {
    let slot = Arc::new(ThreadSlot { accum: Mutex::new(Accum::default()) });
    lock(&THREADS).push(Arc::clone(&slot));
    slot
}

/// A measured function that panics must not leave the probe unusable.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Default, Clone)]
pub(crate) struct Accum {
    pub(crate) edges: IntMap<(u32, u32), u64>,
    pub(crate) calls: IntMap<u32, CallStat>,
    pub(crate) stacks: IntMap<u32, StackStat>,
}

/// One calling context's share of a window.
#[derive(Default, Clone, Copy)]
pub(crate) struct StackStat {
    pub(crate) count: u64,
    pub(crate) self_ns: u64,
}

#[derive(Default, Clone)]
pub(crate) struct CallStat {
    pub(crate) count: u64,
    pub(crate) ns: Samples,
}

/// Durations for one function in one window.
///
/// Raw while the window is small enough for the daemon to bucket them itself,
/// which keeps real nanosecond values on the wire. Past the protocol's cap it
/// folds to buckets and stays there, so one busy window cannot grow without
/// bound.
#[derive(Clone)]
pub(crate) enum Samples {
    Raw(Vec<u64>),
    Buckets(IntMap<u16, u32>),
}

impl Default for Samples {
    fn default() -> Self {
        Samples::Raw(Vec::new())
    }
}

impl Samples {
    fn add(&mut self, value: u64) {
        match self {
            Samples::Raw(values) if values.len() < MAX_RAW_SAMPLES => values.push(value),
            Samples::Raw(values) => {
                let mut buckets: IntMap<u16, u32> = IntMap::default();
                for existing in values.drain(..) {
                    *buckets.entry(bucket_of(existing)).or_insert(0) += 1;
                }
                *buckets.entry(bucket_of(value)).or_insert(0) += 1;
                *self = Samples::Buckets(buckets);
            }
            Samples::Buckets(buckets) => *buckets.entry(bucket_of(value)).or_insert(0) += 1,
        }
    }

    fn into_buckets(self) -> IntMap<u16, u32> {
        match self {
            Samples::Raw(values) => {
                let mut buckets = IntMap::default();
                for value in values {
                    *buckets.entry(bucket_of(value)).or_insert(0) += 1;
                }
                buckets
            }
            Samples::Buckets(buckets) => buckets,
        }
    }

    fn absorb(&mut self, other: Samples) {
        if let (Samples::Raw(mine), Samples::Raw(theirs)) = (&mut *self, &other) {
            if mine.len() + theirs.len() <= MAX_RAW_SAMPLES {
                mine.extend_from_slice(theirs);
                return;
            }
        }
        let mut buckets = std::mem::take(self).into_buckets();
        for (index, count) in other.into_buckets() {
            *buckets.entry(index).or_insert(0) += count;
        }
        *self = Samples::Buckets(buckets);
    }

    pub(crate) fn into_dist(self) -> Dist {
        match self {
            Samples::Raw(values) => Dist::Raw { v: values },
            Samples::Buckets(buckets) => {
                let mut b: Vec<(u16, u32)> = buckets.into_iter().collect();
                b.sort_unstable_by_key(|(index, _)| *index);
                Dist::Buckets { b }
            }
        }
    }
}

impl Accum {
    fn absorb(&mut self, other: Accum) {
        for (edge, count) in other.edges {
            *self.edges.entry(edge).or_insert(0) += count;
        }
        for (func, stat) in other.calls {
            let mine = self.calls.entry(func).or_default();
            mine.count += stat.count;
            mine.ns.absorb(stat.ns);
        }
        for (path, stat) in other.stacks {
            let mine = self.stacks.entry(path).or_default();
            mine.count += stat.count;
            mine.self_ns += stat.self_ns;
        }
    }
}

/// Empties every thread's accumulator into one. Threads that have exited and
/// have nothing left to give stop being visited.
pub(crate) fn drain() -> Accum {
    let mut merged = Accum::default();
    let mut threads = lock(&THREADS);
    threads.retain(|slot| {
        merged.absorb(std::mem::take(&mut *lock(&slot.accum)));
        // The registry holds one reference; a live thread's local holds the
        // other. One reference alone means the thread is gone and drained.
        Arc::strong_count(slot) > 1
    });
    merged
}

/// Everything accumulated so far, leaving it in place. For a process that is
/// asserting on its own behavior rather than emitting it.
pub(crate) fn snapshot() -> Accum {
    let mut merged = Accum::default();
    for slot in lock(&THREADS).iter() {
        merged.absorb(lock(&slot.accum).clone());
    }
    merged
}

/// How many activations are open on this thread. Zero everywhere a measured
/// function is not currently running, including after one panicked.
pub(crate) fn open_activations() -> usize {
    STATE.try_with(|state| state.borrow().stack.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_samples_fold_to_buckets_at_the_protocol_cap() {
        let mut samples = Samples::default();
        for value in 0..MAX_RAW_SAMPLES as u64 {
            samples.add(value);
        }
        assert!(matches!(samples, Samples::Raw(_)), "still raw at the cap");
        samples.add(1);
        let Dist::Buckets { b } = samples.into_dist() else {
            panic!("one sample past the cap must fold to buckets")
        };
        assert_eq!(
            b.iter().map(|(_, count)| *count as u64).sum::<u64>(),
            MAX_RAW_SAMPLES as u64 + 1,
            "folding must not lose a sample"
        );
    }

    #[test]
    fn buckets_come_out_sorted_so_the_wire_shape_is_stable() {
        let mut samples = Samples::Buckets(IntMap::default());
        for value in [900u64, 4, 44_720_000, 17] {
            samples.add(value);
        }
        let Dist::Buckets { b } = samples.into_dist() else { panic!("expected buckets") };
        let mut sorted = b.clone();
        sorted.sort_unstable_by_key(|(index, _)| *index);
        assert_eq!(b, sorted);
    }

    #[test]
    fn absorbing_a_bucketed_accumulator_keeps_every_sample() {
        let mut left = Samples::Buckets(IntMap::default());
        left.add(100);
        let mut right = Samples::Buckets(IntMap::default());
        right.add(100);
        right.add(200);
        left.absorb(right);
        assert_eq!(left.into_dist().count(), 3);
    }
}
