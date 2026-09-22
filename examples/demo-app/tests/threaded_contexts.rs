//! Alone in its process: it counts the contexts the process has named, and
//! the point of the test is that two threads agree on that number.
#![cfg(feature = "enabled")]

use demo_app::a;
use lightwatch::testing;

#[test]
fn two_threads_running_the_same_chain_share_its_contexts() {
    let workers: Vec<_> = (0..2).map(|_| std::thread::spawn(|| a())).collect();
    for worker in workers {
        worker.join().expect("a measured worker should not panic");
    }

    // The tree is process-wide by design: a flame graph aggregates threads.
    // Two threads interning the same chain concurrently must converge on one
    // set of ids, not race into two parallel trees.
    assert_eq!(
        testing::path_count(),
        3,
        "a -> b -> c is three contexts however many threads walked it"
    );

    let stacks = testing::stacks();
    assert_eq!(
        stacks[&vec!["a", "b", "c"]].0,
        2,
        "both threads' work lands on the one shared context"
    );
}
