//! Emit lightwatch frames from a running Rust program.
//!
//! Two things this answers that a timing profiler does not: **which function
//! called which**, and **how many objects of a given type are alive**. Timings
//! are collected only because a frame carries them.
//!
//! ```
//! use lightwatch_probe as lightwatch;
//!
//! #[lightwatch::track]
//! struct Pixel {
//!     rgba: [u8; 4],
//! }
//!
//! #[lightwatch::measure]
//! fn decode(bytes: &[u8]) -> Vec<Pixel> {
//!     bytes
//!         .chunks(4)
//!         .map(|c| Pixel::new_tracked(PixelFields { rgba: [c[0], c[1], c[2], 255] }))
//!         .collect()
//! }
//!
//! lightwatch::start_named("my-app");
//! let pixels = decode(&[1, 2, 3, 4, 5, 6, 7, 8]);
//! assert_eq!(pixels.len(), 2);
//! ```
//!
//! # Read this before you trust a census
//!
//! [`track`] generates a [`Measured`] implementation that returns
//! `size_of::<Self>()`. **For a type whose interesting content is a heap
//! buffer, that number is worthless.** A `Thumbnail { pixels: Vec<u8> }` is 24
//! bytes by `size_of` and several megabytes in reality, and a histogram of 24s
//! tells nobody anything. Every heap-owning tracked type should say so:
//!
//! ```
//! use lightwatch_probe as lightwatch;
//!
//! #[lightwatch::track(manual_measured)]
//! struct Thumbnail {
//!     pixels: Vec<u8>,
//! }
//!
//! impl lightwatch::Measured for Thumbnail {
//!     fn bytes(&self) -> usize {
//!         size_of::<Self>() + self.pixels.capacity()
//!     }
//! }
//!
//! # #[cfg(feature = "enabled")] {
//! let thumbnail = Thumbnail::new_tracked(ThumbnailFields { pixels: Vec::with_capacity(4096) });
//! assert_eq!(lightwatch::testing::live("Thumbnail"), 1);
//! assert!(lightwatch::testing::bytes("Thumbnail") >= 4096, "the heap buffer is counted");
//! drop(thumbnail);
//! assert_eq!(lightwatch::testing::live("Thumbnail"), 0);
//! # }
//! ```
//!
//! `manual_measured` tells [`track`] to leave the implementation to you;
//! without it the generated one would collide with yours.
//!
//! # What the call graph shows
//!
//! Only edges where **both** ends carry [`measure`] are visible. An
//! uninstrumented function between two measured ones collapses into a direct
//! edge from the outer to the inner. That is the intended reading: the graph
//! describes the measured program, not the whole program.
//!
//! Recursion is recorded once per outermost entry. A function that calls
//! itself twenty deep contributes one call, one duration covering the whole
//! outermost activation, and one `f -> f` edge. Counting each level would
//! multiply both the call count and the time.
//!
//! `async fn` is rejected at compile time. A future polled on one thread and
//! resumed on another would attribute its edges to whatever happened to be on
//! the second thread's stack, and a silently wrong graph is worse than no
//! graph.
//!
//! # Cost when the feature is off
//!
//! Without the `enabled` feature, [`measure`] returns the function body
//! unchanged, [`Census`] is a zero-sized type with no `Drop`, and none of the
//! recording or transport code is compiled at all.

// Lets the generated code say `::lightwatch_probe::...` even inside this
// crate's own tests and doctests, where `crate::` would mean the test.
extern crate self as lightwatch_probe;

pub mod census;

pub use census::{Census, Measured, Tracked, TypeSlot};
pub use lightwatch_probe_macros::{measure, track};

#[cfg(feature = "enabled")]
mod calls;
#[cfg(feature = "enabled")]
mod hashing;
#[cfg(feature = "enabled")]
mod emit;
#[cfg(feature = "enabled")]
mod registry;
#[cfg(feature = "enabled")]
pub mod testing;

#[cfg(feature = "enabled")]
pub use calls::{Guard, Site};
#[cfg(feature = "enabled")]
pub use emit::{start, start_named};

/// No-op: the `enabled` feature is off, so there is nothing to emit.
#[cfg(not(feature = "enabled"))]
pub fn start() {}

/// No-op: the `enabled` feature is off, so there is nothing to emit.
#[cfg(not(feature = "enabled"))]
pub fn start_named(_app: impl Into<String>) {}
