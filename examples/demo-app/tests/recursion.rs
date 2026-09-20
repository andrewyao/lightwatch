#![cfg(feature = "enabled")]

use std::time::Instant;

use demo_app::descend;
use lightwatch::testing;

const LEVELS: u32 = 8;

#[test]
fn recursion_is_counted_once_per_outermost_entry_and_timed_once() {
    let started = Instant::now();
    descend(LEVELS);
    let wall = started.elapsed().as_nanos() as u64;

    assert_eq!(
        testing::edge_counts().get(&("descend", "descend")),
        Some(&1),
        "nine nested activations must leave one self-edge, not nine"
    );
    assert_eq!(testing::call_count("descend"), 1);

    let durations = testing::call_durations("descend");
    assert_eq!(durations.len(), 1, "only the outermost activation is timed");
    // Each level sleeps a millisecond, so counting every level would report
    // roughly 9+8+...+1 milliseconds where the call really took 9.
    assert!(
        durations[0] <= wall * 2,
        "{} ns against {} ns of wall time: nested time is being counted more than once",
        durations[0],
        wall
    );
    assert!(
        durations[0] * 2 >= wall,
        "{} ns against {} ns of wall time: the outermost span is being cut short",
        durations[0],
        wall
    );

    descend(2);
    assert_eq!(
        testing::edge_counts().get(&("descend", "descend")),
        Some(&2),
        "a second outermost entry adds exactly one more self-edge"
    );
    assert_eq!(testing::call_count("descend"), 2);
}
