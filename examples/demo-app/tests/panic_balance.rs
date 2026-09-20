#![cfg(feature = "enabled")]

use std::panic::{catch_unwind, AssertUnwindSafe};

use demo_app::{a, explodes};
use lightwatch::testing;

#[test]
fn a_panic_closes_its_activation_instead_of_leaving_the_stack_open() {
    assert_eq!(testing::open_activations(), 0);

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = catch_unwind(AssertUnwindSafe(explodes));
    std::panic::set_hook(previous);

    assert!(outcome.is_err(), "the test subject has to actually panic");
    assert_eq!(testing::open_activations(), 0, "the unwind left an activation open");
    assert_eq!(
        testing::call_count("explodes"),
        1,
        "an activation that unwound still closed, so it still counts as a call"
    );

    // The real damage an unbalanced stack does is silent: the next call gets
    // attributed to whatever was left behind.
    a();
    let edges = testing::edges();
    assert!(
        !edges.iter().any(|(_, to)| *to == "a"),
        "`a` was called from the test, not from a measured function, but the probe \
         gave it a caller: {edges:?}"
    );
    assert!(edges.contains(&("a", "b")), "measurement still works after the panic");
}
