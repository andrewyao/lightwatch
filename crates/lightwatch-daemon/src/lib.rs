//! The lightwatch daemon.
//!
//! [`ingest`] is the system boundary: it reads newline-delimited protocol
//! streams off a unix socket and refuses anything it cannot vouch for.
//! [`store`] holds what survived, per process, forever for the call graph and
//! in bounded rings for the time series. [`api`] and [`server`] hand that to a
//! web UI. [`quantity`] is why a census can never be added to itself.

pub mod api;
pub mod ingest;
pub mod quantity;
pub mod ring;
pub mod server;
pub mod store;
