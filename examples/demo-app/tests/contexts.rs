//! Alone in its process: it counts the contexts one function was reached
//! through, and a parallel test reaching it another way would add one.
#![cfg(feature = "enabled")]

use demo_app::{a, pipeline, stages};
use lightwatch::testing;

#[test]
fn the_same_function_under_two_callers_is_two_contexts() {
    a();
    pipeline(&stages());

    let stacks = testing::stacks();
    let reaching_c: Vec<&Vec<&str>> =
        stacks.keys().filter(|chain| chain.last() == Some(&"c")).collect();
    assert_eq!(
        reaching_c.len(),
        2,
        "c is reached two ways and is two contexts, not one row: {reaching_c:?}"
    );

    // The whole point of splitting them: each caller's share is its own, and
    // an edge could never have said which was which.
    assert_eq!(stacks[&vec!["a", "b", "c"]].0, 1);
    assert_eq!(stacks[&vec!["pipeline", "Decode::run", "c"]].0, 1);
}
