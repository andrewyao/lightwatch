//! The lightwatch daemon.
//!
//! [`ingest`] is the system boundary: it reads newline-delimited protocol
//! streams off a unix socket and refuses anything it cannot vouch for.
//! [`store`] holds what survived, forever for the call graph and in bounded
//! [`ring`]s for the time series. [`quantity`] is why a census can never be
//! added to itself.

pub mod ingest;
pub mod quantity;
pub mod ring;
pub mod store;
