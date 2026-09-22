//! A workload shaped to produce every event the lightwatch protocol carries,
//! so the daemon and the UI have a real feed without a photo editor attached.
//!
//! The shapes it builds, one function per thing the protocol has to survive:
//!
//! ```text
//!   a ──▶ b ──▶ c ◀── Decode::run ◀── pipeline ──▶ Encode::run
//!   render ──▶ paint            (through an uninstrumented `blit`)
//!   descend ──▶ descend         (one self-edge however deep it goes)
//! ```
//!
//! [`import`] is the other half: an ordinary pipeline, seven frames deep and
//! branching, so an interface has proportions to draw rather than seven
//! functions in a row.
//!
//! `pipeline` reaches its two stages through `Box<dyn Stage>`. Nothing a
//! compiler could see at the call site says which one runs, which is the whole
//! reason these edges are collected at runtime.

use std::hint::black_box;
use std::time::Duration;

use lightwatch::{measure, track};

pub mod import;

#[measure]
pub fn a() -> u64 {
    b()
}

#[measure]
fn b() -> u64 {
    c()
}

/// The leaf both `b` and `Decode::run` reach, so the graph has a join in it.
#[measure]
fn c() -> u64 {
    black_box((0..512u64).map(|n| n.wrapping_mul(2_654_435_761)).sum())
}

/// One step of a pipeline. Called only through `Box<dyn Stage>`.
pub trait Stage {
    fn run(&self) -> u64;
}

pub struct Decode;

impl Stage for Decode {
    // Both implementations are called `run`; the explicit names are what keeps
    // them apart in the graph.
    #[measure(name = "Decode::run")]
    fn run(&self) -> u64 {
        c()
    }
}

pub struct Encode;

impl Stage for Encode {
    #[measure(name = "Encode::run")]
    fn run(&self) -> u64 {
        black_box((0..256u64).map(|n| n ^ 0x9e37).sum())
    }
}

pub fn stages() -> Vec<Box<dyn Stage>> {
    vec![Box::new(Decode), Box::new(Encode)]
}

#[measure]
pub fn pipeline(stages: &[Box<dyn Stage>]) -> u64 {
    stages.iter().map(|stage| stage.run()).sum()
}

#[measure]
pub fn render() -> u64 {
    blit()
}

/// Deliberately not measured. The edge the probe records is `render -> paint`,
/// because an uninstrumented frame between two measured ones collapses.
fn blit() -> u64 {
    paint()
}

#[measure]
fn paint() -> u64 {
    black_box(7)
}

/// Sleeps one millisecond per level, so a caller can tell a duration covering
/// the outermost activation from one that added every level up.
#[measure]
pub fn descend(depth: u32) -> u64 {
    std::thread::sleep(Duration::from_millis(1));
    match depth {
        0 => 1,
        _ => descend(depth - 1) + 1,
    }
}

#[measure]
pub fn explodes() {
    panic!("a measured function that panics must still close its activation");
}

/// A tracked type that owns nothing. `size_of` is the truth here, so the
/// generated `Measured` is the right one.
#[track]
pub struct Tag {
    pub id: u32,
    pub weight: u8,
}

/// A tracked type that is essentially its heap buffer.
#[track(manual_measured)]
pub struct Thumbnail {
    pub pixels: Vec<u8>,
}

impl lightwatch::Measured for Thumbnail {
    fn bytes(&self) -> usize {
        size_of::<Self>() + self.pixels.capacity()
    }
}

#[measure]
pub fn build_tags(count: usize) -> Vec<Tag> {
    (0..count)
        .map(|index| Tag::new_tracked(TagFields { id: index as u32, weight: 1 }))
        .collect()
}

#[measure]
pub fn build_thumbnails(count: usize, pixel_bytes: usize) -> Vec<Thumbnail> {
    (0..count)
        .map(|_| Thumbnail::new_tracked(ThumbnailFields { pixels: vec![0u8; pixel_bytes] }))
        .collect()
}

/// One pass over the whole graph.
pub fn one_round() -> u64 {
    a().wrapping_add(pipeline(&stages()))
        .wrapping_add(render())
        .wrapping_add(descend(4))
        .wrapping_add(import::run(3, 256))
}
