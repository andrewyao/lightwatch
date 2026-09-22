//! Time-bucketed windows, newest at the back.
//!
//! A window's span comes from the frame's own `t_ns`, not from the emitter's
//! declared `window_ms`. An emitter that picks 10ms windows then folds ten
//! frames into one fine window instead of handing the UI a different time axis
//! per process.

use std::collections::{BTreeMap, VecDeque};

use lightwatch_proto::{FunctionId, PathId, TypeId};

use crate::quantity::{Absolute, Cumulative};

pub const FINE_RESOLUTION_NS: u64 = 100_000_000;
pub const FINE_CAPACITY: usize = 600;
pub const COARSE_RESOLUTION_NS: u64 = 1_000_000_000;
pub const COARSE_CAPACITY: usize = 900;

/// One function's share of a window. Both fields accumulate.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CallDelta {
    pub calls: Cumulative,
    pub ns: Cumulative,
}

/// One calling context's share of a window. Both fields are deltas.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StackDelta {
    pub calls: Cumulative,
    /// Time spent in this context and outside any measured callee, so summing
    /// a subtree gives inclusive time without counting a nanosecond twice.
    pub self_ns: Cumulative,
}

/// One type's live-instance reading. Every field is absolute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CensusReading {
    pub live: Absolute,
    pub bytes: Absolute,
    pub size_buckets: Vec<(u16, u32)>,
    /// When the reading was taken, so a later one wins regardless of arrival
    /// order.
    pub at_t_ns: u64,
}

#[derive(Debug, Clone)]
pub struct Window {
    pub index: u64,
    pub resolution_ns: u64,
    pub calls: BTreeMap<FunctionId, CallDelta>,
    pub edges: BTreeMap<(FunctionId, FunctionId), Cumulative>,
    pub stacks: BTreeMap<PathId, StackDelta>,
    pub census: BTreeMap<TypeId, CensusReading>,
}

impl Window {
    fn new(index: u64, resolution_ns: u64) -> Self {
        Window {
            index,
            resolution_ns,
            calls: BTreeMap::new(),
            edges: BTreeMap::new(),
            stacks: BTreeMap::new(),
            census: BTreeMap::new(),
        }
    }

    pub fn start_t_ns(&self) -> u64 {
        self.index.saturating_mul(self.resolution_ns)
    }

    pub fn end_t_ns(&self) -> u64 {
        self.start_t_ns().saturating_add(self.resolution_ns)
    }

    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
            && self.edges.is_empty()
            && self.stacks.is_empty()
            && self.census.is_empty()
    }

    pub fn add_calls(&mut self, func: FunctionId, calls: u64, ns: u64) {
        let entry = self.calls.entry(func).or_default();
        entry.calls.accumulate(calls);
        entry.ns.accumulate(ns);
    }

    pub fn add_edge(&mut self, from: FunctionId, to: FunctionId, calls: u64) {
        self.edges.entry((from, to)).or_default().accumulate(calls);
    }

    pub fn add_stack(&mut self, path: PathId, calls: u64, self_ns: u64) {
        let entry = self.stacks.entry(path).or_default();
        entry.calls.accumulate(calls);
        entry.self_ns.accumulate(self_ns);
    }

    pub fn record_census(&mut self, ty: TypeId, reading: CensusReading) {
        let newer_already_held = self
            .census
            .get(&ty)
            .is_some_and(|held| held.at_t_ns > reading.at_t_ns);
        if !newer_already_held {
            self.census.insert(ty, reading);
        }
    }
}

/// A fixed-length run of contiguous windows.
#[derive(Debug)]
pub struct Ring {
    resolution_ns: u64,
    capacity: usize,
    windows: VecDeque<Window>,
}

impl Ring {
    pub fn new(resolution_ns: u64, capacity: usize) -> Self {
        assert!(resolution_ns > 0 && capacity > 0);
        Ring {
            resolution_ns,
            capacity,
            windows: VecDeque::with_capacity(capacity.min(64)),
        }
    }

    pub fn fine() -> Self {
        Ring::new(FINE_RESOLUTION_NS, FINE_CAPACITY)
    }

    pub fn coarse() -> Self {
        Ring::new(COARSE_RESOLUTION_NS, COARSE_CAPACITY)
    }

    pub fn resolution_ns(&self) -> u64 {
        self.resolution_ns
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    pub fn index_of(&self, t_ns: u64) -> u64 {
        t_ns / self.resolution_ns
    }

    pub fn newest_index(&self) -> Option<u64> {
        self.windows.back().map(|w| w.index)
    }

    pub fn oldest_index(&self) -> Option<u64> {
        self.windows.front().map(|w| w.index)
    }

    /// Windows oldest first.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Window> {
        self.windows.iter()
    }

    /// The last `count` windows, oldest first.
    pub fn most_recent(&self, count: usize) -> impl DoubleEndedIterator<Item = &Window> {
        let skip = self.windows.len().saturating_sub(count);
        self.windows.iter().skip(skip)
    }

    pub fn get(&self, index: u64) -> Option<&Window> {
        let oldest = self.oldest_index()?;
        let offset = index.checked_sub(oldest)?;
        self.windows.get(usize::try_from(offset).ok()?)
    }

    /// The window covering `t_ns`, materializing it and any idle windows
    /// between it and the newest one so the ring stays a contiguous time axis.
    ///
    /// `None` when `t_ns` predates the oldest window the ring still holds: a
    /// frame that late has nowhere to land and the caller counts it.
    pub fn window_for(&mut self, t_ns: u64) -> Option<&mut Window> {
        let index = self.index_of(t_ns);
        match self.windows.back().map(|w| w.index) {
            None => self.windows.push_back(Window::new(index, self.resolution_ns)),
            Some(newest) if index > newest => {
                let gap = index - newest;
                if gap as usize >= self.capacity {
                    self.windows.clear();
                    self.windows.push_back(Window::new(index, self.resolution_ns));
                } else {
                    for missing in (newest + 1)..=index {
                        self.windows.push_back(Window::new(missing, self.resolution_ns));
                    }
                }
            }
            Some(_) => {
                if index < self.windows.front().map(|w| w.index).unwrap_or(index) {
                    return None;
                }
            }
        }
        while self.windows.len() > self.capacity {
            self.windows.pop_front();
        }
        let oldest = self.windows.front()?.index;
        let offset = usize::try_from(index.checked_sub(oldest)?).ok()?;
        self.windows.get_mut(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn func(id: u32) -> FunctionId {
        FunctionId(id)
    }

    #[test]
    fn a_window_holds_the_frames_that_fall_inside_its_span() {
        let mut ring = Ring::new(100, 4);
        ring.window_for(0).unwrap().add_calls(func(1), 2, 20);
        ring.window_for(50).unwrap().add_calls(func(1), 3, 30);
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.get(0).unwrap().calls[&func(1)].calls.get(), 5);
    }

    #[test]
    fn the_ring_evicts_the_oldest_window_once_it_is_full() {
        let mut ring = Ring::new(100, 3);
        for window in 0..5u64 {
            ring.window_for(window * 100).unwrap().add_calls(func(1), window, 0);
        }
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.oldest_index(), Some(2));
        assert_eq!(ring.newest_index(), Some(4));
        assert!(ring.get(0).is_none(), "the oldest window must be gone, not merged");
        assert!(ring.get(1).is_none());
        assert_eq!(ring.get(2).unwrap().calls[&func(1)].calls.get(), 2);
    }

    #[test]
    fn an_idle_stretch_materializes_as_empty_windows_rather_than_a_hidden_jump() {
        let mut ring = Ring::new(100, 10);
        ring.window_for(0).unwrap().add_calls(func(1), 1, 0);
        ring.window_for(400).unwrap().add_calls(func(1), 1, 0);
        assert_eq!(ring.len(), 5);
        assert!(ring.get(2).unwrap().is_empty());
    }

    #[test]
    fn an_idle_stretch_longer_than_the_ring_restarts_it_instead_of_materializing_the_gap() {
        let mut ring = Ring::new(100, 4);
        ring.window_for(0).unwrap().add_calls(func(1), 1, 0);
        ring.window_for(10_000).unwrap().add_calls(func(1), 1, 0);
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.newest_index(), Some(100));
    }

    #[test]
    fn a_frame_older_than_the_ring_has_nowhere_to_land() {
        let mut ring = Ring::new(100, 2);
        for window in 0..4u64 {
            ring.window_for(window * 100);
        }
        assert!(ring.window_for(0).is_none());
        assert!(ring.window_for(200).is_some());
    }

    #[test]
    fn a_census_in_a_window_keeps_the_latest_reading_not_the_sum() {
        let mut ring = Ring::new(1000, 4);
        let window = ring.window_for(0).unwrap();
        window.record_census(
            TypeId(1),
            CensusReading {
                live: Absolute::reading(3),
                bytes: Absolute::reading(300),
                size_buckets: vec![],
                at_t_ns: 100,
            },
        );
        window.record_census(
            TypeId(1),
            CensusReading {
                live: Absolute::reading(5),
                bytes: Absolute::reading(500),
                size_buckets: vec![],
                at_t_ns: 200,
            },
        );
        assert_eq!(window.census[&TypeId(1)].live.get(), 5);
        assert_eq!(window.census[&TypeId(1)].bytes.get(), 500);
    }

    #[test]
    fn a_census_that_arrives_out_of_order_does_not_undo_a_newer_one() {
        let mut ring = Ring::new(1000, 4);
        let window = ring.window_for(0).unwrap();
        window.record_census(
            TypeId(1),
            CensusReading {
                live: Absolute::reading(5),
                bytes: Absolute::reading(500),
                size_buckets: vec![],
                at_t_ns: 200,
            },
        );
        window.record_census(
            TypeId(1),
            CensusReading {
                live: Absolute::reading(3),
                bytes: Absolute::reading(300),
                size_buckets: vec![],
                at_t_ns: 100,
            },
        );
        assert_eq!(window.census[&TypeId(1)].live.get(), 5);
    }
}
