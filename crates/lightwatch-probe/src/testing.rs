//! Reading the probe's own state from inside the measured process.
//!
//! Everything here is a snapshot that leaves the accumulators in place, so a
//! test observing them does not steal another test's data. A process that has
//! called [`start`](crate::start) has an emit thread draining those same
//! accumulators, so use one or the other, not both.

use std::collections::{BTreeMap, BTreeSet};

use crate::{calls, registry};

/// Every recorded caller/callee pair, by registered name.
pub fn edges() -> BTreeSet<(&'static str, &'static str)> {
    calls::snapshot()
        .edges
        .keys()
        .filter_map(|(from, to)| {
            Some((registry::function_name(*from)?, registry::function_name(*to)?))
        })
        .collect()
}

/// How many times each pair was recorded.
pub fn edge_counts() -> BTreeMap<(&'static str, &'static str), u64> {
    calls::snapshot()
        .edges
        .iter()
        .filter_map(|((from, to), count)| {
            Some(((registry::function_name(*from)?, registry::function_name(*to)?), *count))
        })
        .collect()
}

/// Closed calls of a function, by registered name. Zero for a function that
/// has never run, and zero for one that only ever ran as a recursive level of
/// itself.
pub fn call_count(name: &str) -> u64 {
    let Some(id) = registry::function_id_by_name(name) else { return 0 };
    calls::snapshot().calls.get(&id.0).map_or(0, |stat| stat.count)
}

/// Recorded durations for a function, in nanoseconds.
pub fn call_durations(name: &str) -> Vec<u64> {
    let Some(id) = registry::function_id_by_name(name) else { return Vec::new() };
    match calls::snapshot().calls.get(&id.0) {
        Some(stat) => match stat.ns.clone().into_dist() {
            lightwatch_proto::Dist::Raw { v } => v,
            lightwatch_proto::Dist::Buckets { b } => b
                .into_iter()
                .flat_map(|(index, count)| {
                    std::iter::repeat_n(lightwatch_proto::bucket_range(index).0, count as usize)
                })
                .collect(),
        },
        None => Vec::new(),
    }
}

/// Live instances of a tracked type, by type name.
pub fn live(type_name: &str) -> u64 {
    registry::type_slot_by_name(type_name).map_or(0, |slot| slot.live())
}

/// The live-size histogram of a tracked type, sparse and sorted.
pub fn size_buckets(type_name: &str) -> Vec<(u16, u32)> {
    registry::type_slot_by_name(type_name).map_or_else(Vec::new, |slot| slot.size_buckets())
}

/// Total footprint of a tracked type's live instances.
pub fn bytes(type_name: &str) -> u64 {
    crate::census::bytes_from_buckets(&size_buckets(type_name))
}

/// Activations open on the calling thread. Zero unless a measured function is
/// on the stack right now.
pub fn open_activations() -> usize {
    calls::open_activations()
}
