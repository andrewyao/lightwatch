//! Alone in its process: it asserts on the self time held by `descend`'s two
//! contexts, which any other call to it would move.
#![cfg(feature = "enabled")]

use demo_app::descend;
use lightwatch::testing;

#[test]
fn every_level_of_a_recursive_call_is_timed_once_and_only_once() {
    // descend sleeps a millisecond per level, so five levels is about 5ms and
    // the outermost level's own share is about one of them.
    descend(4);

    let stacks = testing::stacks();
    let (_, outermost_self) = stacks[&vec!["descend"]];
    let (_, levels_self) = stacks[&vec!["descend", "descend"]];
    let inclusive = testing::call_durations("descend")[0];

    assert_eq!(
        outermost_self + levels_self,
        inclusive,
        "every level's self time together is the one duration the call reported"
    );
    // Four inner levels against one outer, so the shared context holds the
    // bulk. Sleep is imprecise, so this is a shape assertion, not a ratio.
    assert!(
        levels_self > outermost_self * 2,
        "four inner levels ({levels_self}ns) should outweigh the outermost ({outermost_self}ns)"
    );
}
