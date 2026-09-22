//! Alone in its process: it asserts on which functions exist in the tree.
#![cfg(feature = "enabled")]

use lightwatch::testing;

#[lightwatch::measure_all]
mod blanket {
    /// Measured because the module was.
    pub fn outer() -> u64 {
        inner() + opted_out() + already_named() + at_compile_time()
    }

    pub fn inner() -> u64 {
        7
    }

    /// Opting out in place, next to the reason, rather than by being moved
    /// out of the module.
    #[lightwatch::measure(skip)]
    pub fn opted_out() -> u64 {
        1
    }

    /// Already measured under its own name, which the blanket must not
    /// double up or overwrite.
    #[lightwatch::measure(name = "blanket::renamed")]
    pub fn already_named() -> u64 {
        2
    }

    /// Cannot be measured. A whole module must not fail to build over it.
    pub const fn at_compile_time() -> u64 {
        3
    }
}

#[test]
fn a_module_attribute_measures_its_functions_and_steps_over_what_it_cannot() {
    assert_eq!(blanket::outer(), 13);

    let stacks = testing::stacks();
    let reached: Vec<&Vec<&str>> = stacks.keys().collect();

    assert!(stacks.contains_key(&vec!["outer", "inner"]), "the blanket measured both: {reached:?}");
    assert!(
        stacks.contains_key(&vec!["outer", "blanket::renamed"]),
        "an explicit #[measure] keeps its own name rather than being overwritten: {reached:?}"
    );
    assert!(
        !reached.iter().any(|chain| chain.contains(&"opted_out")),
        "`skip` left it out: {reached:?}"
    );
    assert!(
        !reached.iter().any(|chain| chain.contains(&"at_compile_time")),
        "a const fn cannot be measured, and must not have stopped the module building: {reached:?}"
    );
    assert_eq!(testing::call_count("inner"), 1, "measured once, not twice");
}
