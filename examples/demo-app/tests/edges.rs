//! Each behavioral test gets its own file, so it runs in its own process and
//! can assert on the complete edge map rather than on a subset of one shared
//! between tests.
#![cfg(feature = "enabled")]

use std::collections::BTreeSet;

use demo_app::{a, pipeline, render, stages};
use lightwatch::testing;

#[test]
fn the_edge_map_holds_exactly_the_pairs_that_ran() {
    a();
    pipeline(&stages());
    render();

    let expected: BTreeSet<(&str, &str)> = [
        ("a", "b"),
        ("b", "c"),
        ("pipeline", "Decode::run"),
        ("pipeline", "Encode::run"),
        // Decode::run also reaches the shared leaf, so the graph has a join.
        ("Decode::run", "c"),
        // `render` calls the uninstrumented `blit`, which calls `paint`. An
        // uninstrumented frame collapses into a direct edge.
        ("render", "paint"),
    ]
    .into_iter()
    .collect();

    assert_eq!(testing::edges(), expected);
}

#[test]
fn a_call_through_dyn_trait_records_the_implementation_that_actually_ran() {
    pipeline(&stages());

    let edges = testing::edges();
    // Nothing at the call site in `pipeline` names either implementation. A
    // compile-time pass would see one edge to `<dyn Stage>::run`; only running
    // the program tells you both of these happened.
    assert!(
        edges.contains(&("pipeline", "Decode::run")),
        "missing the dynamically dispatched edge to Decode, got {edges:?}"
    );
    assert!(
        edges.contains(&("pipeline", "Encode::run")),
        "missing the dynamically dispatched edge to Encode, got {edges:?}"
    );
}
