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
    edge_counts().into_keys().collect()
}

/// How many times each pair was recorded.
///
/// Read off the call tree: a context names a function and the context that
/// reaches it, so the edge is already there and the weight is how often that
/// context was entered.
pub fn edge_counts() -> BTreeMap<(&'static str, &'static str), u64> {
    let recorded = calls::snapshot().stacks;
    let func_of: BTreeMap<u32, u32> =
        registry::path_nodes().into_iter().map(|(id, _, func)| (id, func)).collect();

    let mut counts: BTreeMap<(&'static str, &'static str), u64> = BTreeMap::new();
    for (id, parent, func) in registry::path_nodes() {
        if parent == 0 {
            continue;
        }
        // A context exists from the moment it is entered; an edge exists once
        // something ran through it, which is what the old edge counter meant.
        let Some(stat) = recorded.get(&id) else { continue };
        let Some(caller) = func_of.get(&parent).copied() else { continue };
        let (Some(from), Some(to)) =
            (registry::function_name(caller), registry::function_name(func))
        else {
            continue;
        };
        *counts.entry((from, to)).or_insert(0) += stat.count;
    }
    counts
}

/// Every calling context recorded so far, keyed by the chain of function
/// names from the outermost measured frame down to the context itself.
///
/// The value is `(calls, self_ns)`: how many activations opened that context,
/// and how long they spent outside any measured callee.
pub fn stacks() -> BTreeMap<Vec<&'static str>, (u64, u64)> {
    let recorded = calls::snapshot().stacks;
    let nodes: BTreeMap<u32, (u32, u32)> =
        registry::path_nodes().into_iter().map(|(id, parent, func)| (id, (parent, func))).collect();

    nodes
        .keys()
        .filter_map(|id| {
            let chain = chain_of(*id, &nodes)?;
            let stat = recorded.get(id)?;
            Some((chain, (stat.count, stat.self_ns)))
        })
        .collect()
}

/// The chain of names leading to one context, outermost first.
fn chain_of(mut id: u32, nodes: &BTreeMap<u32, (u32, u32)>) -> Option<Vec<&'static str>> {
    let mut chain = Vec::new();
    while id != 0 {
        let (parent, func) = *nodes.get(&id)?;
        chain.push(registry::function_name(func)?);
        id = parent;
    }
    chain.reverse();
    Some(chain)
}

/// How many distinct calling contexts this process has named.
///
/// The number a recursive function is allowed to add is what keeps a call
/// tree finite, so it is worth asserting on directly.
pub fn path_count() -> usize {
    registry::path_nodes().len()
}

/// The deepest chain in the call tree.
pub fn deepest_chain() -> usize {
    let nodes: BTreeMap<u32, (u32, u32)> =
        registry::path_nodes().into_iter().map(|(id, parent, func)| (id, (parent, func))).collect();
    nodes.keys().filter_map(|id| chain_of(*id, &nodes)).map(|chain| chain.len()).max().unwrap_or(0)
}

/// Contexts refused because the table was full. Non-zero means the flame
/// graph is shallower than the program actually was.
pub fn paths_overflowed() -> u64 {
    registry::paths_overflowed()
}

/// Time spent in a function and outside any measured callee, summed over
/// every context it was reached through.
pub fn self_ns(name: &str) -> u64 {
    let Some(id) = registry::function_id_by_name(name) else { return 0 };
    let recorded = calls::snapshot().stacks;
    registry::path_nodes()
        .into_iter()
        .filter(|(_, _, func)| *func == id.0)
        .filter_map(|(path, _, _)| recorded.get(&path).map(|stat| stat.self_ns))
        .sum()
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
