//! Alone in its process: it counts every context the process has named, so
//! anything else running would add to the total.
#![cfg(feature = "enabled")]

use demo_app::descend;
use lightwatch::testing;

#[test]
fn recursion_does_not_deepen_the_call_tree() {
    descend(2);
    let after_shallow = testing::path_count();

    descend(8);
    descend(64);
    let after_deep = testing::path_count();

    assert_eq!(
        after_shallow, after_deep,
        "sixty-four levels named {} contexts that two levels did not. A tree that \
         grows with recursion depth is unbounded, and a flame graph of it is a \
         thousand identical slivers.",
        after_deep - after_shallow
    );
    assert_eq!(
        after_deep, 2,
        "a recursive function is its outermost context plus one shared by every level"
    );
    assert_eq!(testing::deepest_chain(), 2);
    assert_eq!(testing::paths_overflowed(), 0, "nothing here should reach the ceiling");
}
