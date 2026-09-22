//! A bridge from `hotpath`'s HTTP server to the lightwatch protocol.
//!
//! Any process built with `#[hotpath::main]` already serves a JSON report on
//! `127.0.0.1:6770`. This binary polls `/functions_timing` once per window,
//! differences the cumulative counters it finds, and re-emits the change as
//! lightwatch frames on the daemon's Unix socket. The target is not modified,
//! recompiled or aware that anyone is watching.
//!
//! It is a client of the protocol, not a module inside the daemon, so the
//! ingest protocol is the only integration point between the two.
//!
//! # This is a degraded source
//!
//! What it gives up against the Rust probe, all of it inherent to the source:
//!
//! - **No call tree, and so no call graph and no flame graph.** hotpath's
//!   caller stack compiles only under its SQL and HTTP features, and even
//!   there it attributes a query to its nearest measured caller rather than
//!   recording function-to-function calls. The bridge registers no
//!   [`Register::Path`](lightwatch_proto::Register::Path) and emits no
//!   [`Event::Stack`](lightwatch_proto::Event::Stack), so this feed is a list
//!   of functions: nodes with no arrows between them, and no stacks to stack.
//! - **No per-type census.** hotpath counts bytes allocated per function under
//!   `hotpath-alloc`. There is no type, no liveness and no free at any feature
//!   combination, so no [`Event::Census`](lightwatch_proto::Event::Census) is
//!   derivable.
//! - **About four significant figures on durations.** Every number in the
//!   report is a preformatted display string (`"44.72 ms"`). The only duration
//!   that survives a difference is the per-function `total`, and it survives it
//!   to the precision that string carries.
//! - **A mean, not a distribution.** See [`bridge`] for what is emitted in
//!   place of one and why the percentile fields are not used.
//! - **Not every measured function is visible.** hotpath truncates its report
//!   to a row limit and says so, in `total_count` against `included_count`. A
//!   profiled lightphotos measures 17 functions and lists 15, so two of them
//!   reach no interface this bridge feeds. Which two can change between polls,
//!   which is why [`bridge`] never treats a missing row as a quiet one.
//!
//! Its job is to light up the interface against an unmodified application. The
//! Rust probe is the full-fidelity path.

pub mod bridge;
pub mod sink;
pub mod source;
