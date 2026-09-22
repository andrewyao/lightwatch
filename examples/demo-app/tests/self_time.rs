//! Alone in its process: it asserts on the whole call tree, and a parallel
//! test calling any measured function would add rows to it.
#![cfg(feature = "enabled")]

use demo_app::{pipeline, stages};
use lightwatch::testing;

#[test]
fn self_time_over_a_call_tree_sums_to_the_time_the_call_took() {
    pipeline(&stages());

    let over_the_tree: u64 = testing::stacks().values().map(|(_, self_ns)| self_ns).sum();
    let inclusive = testing::call_durations("pipeline")[0];

    // Both numbers come from the same clock reads, so this is not a tolerance
    // on measurement noise. A gap means a callee's time never reached its
    // caller, or was subtracted from it twice.
    let gap = over_the_tree.abs_diff(inclusive);
    assert!(
        gap < 2_000,
        "the tree's self time is {over_the_tree}ns against a call of {inclusive}ns, \
         a gap of {gap}ns: {:?}",
        testing::stacks()
    );
}
