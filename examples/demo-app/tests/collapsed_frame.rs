//! Alone in its process: it asserts that no chain anywhere names `blit`.
#![cfg(feature = "enabled")]

use demo_app::render;
use lightwatch::testing;

#[test]
fn an_uninstrumented_frame_charges_its_time_to_the_nearest_measured_caller() {
    render();

    let stacks = testing::stacks();
    assert!(
        stacks.contains_key(&vec!["render", "paint"]),
        "blit is not measured, so it cannot appear in a chain: {:?}",
        stacks.keys().collect::<Vec<_>>()
    );
    assert!(
        !stacks.keys().any(|chain| chain.contains(&"blit")),
        "an unmeasured frame has no context of its own"
    );

    // render's body is a single call to blit, so what render is charged with
    // is blit's own cost. It has to land somewhere, and the nearest measured
    // caller is the only honest place for it.
    let (_, paint_self) = stacks[&vec!["render", "paint"]];
    let (_, render_self) = stacks[&vec!["render"]];
    assert!(paint_self > 0, "paint did work and must be charged for it");
    assert_eq!(
        render_self + paint_self,
        testing::call_durations("render")[0],
        "the two together are the whole call, with nothing dropped in between"
    );
}
