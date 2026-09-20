//! The two numeric kinds the protocol carries, kept apart by the type system.
//!
//! `Calls` and `Edge` are deltas that accumulate. `Census` is an absolute
//! reading that replaces the previous one. Summing a census is the easiest bug
//! to write against this protocol, so the two never share an addition operator
//! and neither converts into the other. A `Cumulative` grows only through
//! [`Cumulative::accumulate`]; an `Absolute` cannot be mutated at all, only
//! overwritten wholesale by a newer reading.

use serde::Serialize;

/// A quantity that grows by folding in one window's delta after another.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Cumulative(u64);

impl Cumulative {
    pub const ZERO: Cumulative = Cumulative(0);

    /// Folds one window's delta in. Saturating, because the arithmetic runs on
    /// counts a hostile client chooses and a panic here would take down a
    /// connection the boundary is supposed to contain.
    pub fn accumulate(&mut self, delta: u64) {
        self.0 = self.0.saturating_add(delta);
    }

    /// Folds another accumulation of the same quantity in, as when a fine
    /// window's total rolls up into a coarse one.
    pub fn absorb(&mut self, other: Cumulative) {
        self.0 = self.0.saturating_add(other.0);
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// A reading taken at one instant. A newer reading replaces an older one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Absolute(u64);

impl Absolute {
    pub fn reading(value: u64) -> Self {
        Absolute(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cumulative_grows_by_the_deltas_folded_into_it() {
        let mut total = Cumulative::ZERO;
        total.accumulate(3);
        total.accumulate(4);
        assert_eq!(total.get(), 7);
    }

    #[test]
    fn a_cumulative_saturates_rather_than_panicking_on_a_hostile_delta() {
        let mut total = Cumulative::ZERO;
        total.accumulate(u64::MAX);
        total.accumulate(u64::MAX);
        assert_eq!(total.get(), u64::MAX);
    }

    #[test]
    fn a_later_reading_replaces_an_earlier_one_instead_of_joining_it() {
        let mut live = Absolute::reading(3);
        assert_eq!(live.get(), 3);
        live = Absolute::reading(5);
        assert_eq!(live.get(), 5, "an absolute offers no way to combine two readings");
    }
}
