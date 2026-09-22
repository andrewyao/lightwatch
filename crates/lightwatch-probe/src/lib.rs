//! Emit lightwatch frames from a running Rust program.
//!
//! Two things this answers that a timing profiler does not: **through which
//! chain of callers a function was reached**, and **how many objects of a
//! given type are alive**. Timings are collected only because a frame carries
//! them.
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
//! # Measuring a whole module
//!
//! A call graph is only as complete as the functions in it, and reaching a
//! useful node count one attribute at a time is why most instrumented
//! programs have three measured functions and no graph worth looking at.
//! [`measure_all`] takes a `mod` or an `impl` block:
//!
//! ```
//! use lightwatch_probe as lightwatch;
//!
//! #[lightwatch::measure_all]
//! mod thumbnail {
//!     use lightwatch_probe as lightwatch;
//!
//!     pub fn get_or_make(id: u32) -> u32 {
//!         make(id)
//!     }
//!
//!     fn make(id: u32) -> u32 {
//!         id * 2
//!     }
//!
//!     /// Left out on purpose, next to the reason.
//!     #[lightwatch::measure(skip)]
//!     pub fn cheap(id: u32) -> u32 {
//!         id
//!     }
//! }
//!
//! assert_eq!(thumbnail::get_or_make(21), 42);
//! ```
//!
//! Unlike [`measure`], it steps over what it cannot measure rather than
//! failing the build. Being told no about one function you asked for is
//! useful; being told no about a module because one function in it is
//! `async` is not.
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
//! # What the call tree shows
//!
//! Only frames carrying [`measure`] appear. An uninstrumented function between
//! two measured ones collapses, so the inner hangs directly off the outer and
//! the time the uninstrumented frame spent is charged to the outer one. That
//! is the intended reading: the tree describes the measured program, not the
//! whole program, and the nearest measured caller is the only honest place to
//! put the cost of what it called.
//!
//! Each context reports **self time**: the nanoseconds it spent outside any
//! measured callee. Summing a subtree gives that subtree's inclusive time with
//! no nanosecond counted twice, and summing the whole tree gives what the
//! process spent.
//!
//! A graph edge is a reading of the tree rather than a separate count. A
//! context names a function and the context that reached it, so the pair is
//! already there.
//!
//! Recursion is recorded once per outermost entry. A function that calls
//! itself twenty deep contributes one call, one duration covering the whole
//! outermost activation, and one `f -> f` edge. Counting each level would
//! multiply both the call count and the time.
//!
//! It costs the tree exactly one extra node per recursive function: every
//! level shares a context hanging off that function's own outermost one, so
//! twenty deep is two contexts rather than twenty. Hanging it off the
//! outermost node rather than the nearest one bounds a cycle through several
//! functions as well, and **that loses an edge**: `a -> b -> a` records
//! `a -> a` and not `b -> a`, because the inner `a` is parented off `a`'s own
//! outermost context. Nothing in this repository exercises mutual recursion,
//! so nothing will go red if that assumption stops being acceptable.
//!
//! The tree is process-wide, not per-thread. Two threads walking the same
//! chain share its contexts and their self times merge, which is what a flame
//! graph wants and is why a context can never say *which thread*. One
//! consequence reaches any client: a window can hold more self time than it
//! holds wall clock.
//!
//! A process may name only so many contexts before the table stops growing
//! (`LIGHTWATCH_MAX_PATHS`, 65536 by default). Past the ceiling an activation
//! reports against the nearest context that fits, so the tree gets shallower
//! rather than unbounded and every nanosecond is still accounted for.
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
pub use lightwatch_probe_macros::{measure, measure_all, track};

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
