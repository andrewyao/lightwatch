//! Alone in its process: it sets a process-wide ceiling on the call tree,
//! which is read once and then fixed for everything that follows.
#![cfg(feature = "enabled")]

use demo_app::{pipeline, stages};
use lightwatch::testing;

#[test]
fn a_full_context_table_grows_shallower_rather_than_without_bound() {
    // A context costs a row in every window the daemon retains, and a program
    // can reach new ones forever, so the table has a ceiling. Two is small
    // enough that the demo's pipeline runs straight past it.
    unsafe { std::env::set_var("LIGHTWATCH_MAX_PATHS", "2") };

    pipeline(&stages());

    assert_eq!(testing::path_count(), 2, "the ceiling holds");
    assert!(
        testing::paths_overflowed() > 0,
        "a tree that stopped growing has to say so rather than look complete"
    );

    // The refused contexts did not vanish: their activations reported against
    // the nearest context that did fit, so the time is still all there.
    let over_the_tree: u64 = testing::stacks().values().map(|(_, self_ns)| self_ns).sum();
    assert_eq!(
        over_the_tree,
        testing::call_durations("pipeline")[0],
        "a truncated tree still accounts for every nanosecond the call took"
    );
}
