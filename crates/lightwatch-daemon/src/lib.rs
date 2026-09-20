//! The lightwatch daemon.
//!
//! [`store`] holds what each profiled process has reported, forever for the
//! call graph and in bounded [`ring`]s for the time series. [`quantity`] is why
//! a census can never be added to itself.

pub mod quantity;
pub mod ring;
pub mod store;
