//! A photo import, deep enough and branchy enough to be worth drawing.
//!
//! The rest of the crate is one function per shape the protocol carries. This
//! is the opposite: an ordinary pipeline with ordinary helpers, so a flame
//! graph has proportions to show and a call graph has something to lay out.
//!
//! ```text
//!   run ─▶ batch ─▶ one_photo ─┬─▶ scan::header ─▶ scan::magic
//!                              ├─▶ decode::frame ─▶ idct ─▶ butterfly ─▶ rotate
//!                              ├─▶ resize::thumbnail ─▶ box_filter ─▶ sample_row
//!                              ├─▶ encode::thumbnail ─▶ entropy ─▶ rle
//!                              └─▶ index::record ─▶ tokenize ─▶ normalize
//! ```
//!
//! Seven measured frames on the deepest path, and `one_photo` fans out to
//! five subtrees of visibly different cost, which is the shape that makes a
//! flame graph tell you something a sorted list does not.

use std::hint::black_box;

use lightwatch::measure_all;

#[measure_all]
pub mod scan {
    use super::*;

    pub fn header(bytes: &[u8]) -> u64 {
        magic(bytes).wrapping_add(length(bytes))
    }

    pub fn magic(bytes: &[u8]) -> u64 {
        black_box(bytes.iter().take(4).map(|b| *b as u64).fold(0u64, u64::wrapping_add))
    }

    pub fn length(bytes: &[u8]) -> u64 {
        black_box(bytes.len() as u64)
    }
}

#[measure_all]
pub mod decode {
    use super::*;

    pub fn frame(pixels: usize) -> u64 {
        huffman(pixels).wrapping_add(dequantize(pixels)).wrapping_add(idct(pixels))
    }

    pub fn huffman(pixels: usize) -> u64 {
        bit_reader(pixels)
    }

    pub fn bit_reader(pixels: usize) -> u64 {
        black_box((0..pixels as u64).map(|n| n.rotate_left(3)).fold(0u64, u64::wrapping_add))
    }

    pub fn dequantize(pixels: usize) -> u64 {
        black_box((0..pixels as u64).map(|n| n.wrapping_mul(17)).fold(0u64, u64::wrapping_add))
    }

    /// The expensive branch, and the deep one. A flame graph earns its place
    /// by making this obvious without anyone sorting a column.
    pub fn idct(pixels: usize) -> u64 {
        butterfly(pixels).wrapping_add(butterfly(pixels / 2))
    }

    pub fn butterfly(pixels: usize) -> u64 {
        rotate(pixels).wrapping_add(black_box((0..pixels as u64).map(|n| n ^ 0x5a5a).fold(0u64, u64::wrapping_add)))
    }

    pub fn rotate(pixels: usize) -> u64 {
        black_box((0..pixels as u64).map(|n| n.rotate_right(7)).fold(0u64, u64::wrapping_add))
    }
}

#[measure_all]
pub mod resize {
    use super::*;

    pub fn thumbnail(pixels: usize) -> u64 {
        box_filter(pixels).wrapping_add(sharpen(pixels))
    }

    pub fn box_filter(pixels: usize) -> u64 {
        (0..4).map(|row| sample_row(pixels / 4, row)).fold(0u64, u64::wrapping_add)
    }

    pub fn sample_row(width: usize, row: u64) -> u64 {
        black_box((0..width as u64).map(|n| n.wrapping_add(row)).fold(0u64, u64::wrapping_add))
    }

    pub fn sharpen(pixels: usize) -> u64 {
        convolve(pixels)
    }

    pub fn convolve(pixels: usize) -> u64 {
        black_box((0..pixels as u64).map(|n| n.wrapping_mul(3)).fold(0u64, u64::wrapping_add))
    }
}

#[measure_all]
pub mod encode {
    use super::*;

    pub fn thumbnail(pixels: usize) -> u64 {
        quantize(pixels).wrapping_add(entropy(pixels))
    }

    pub fn quantize(pixels: usize) -> u64 {
        black_box((0..pixels as u64).map(|n| n / 3).fold(0u64, u64::wrapping_add))
    }

    pub fn entropy(pixels: usize) -> u64 {
        rle(pixels)
    }

    pub fn rle(pixels: usize) -> u64 {
        black_box((0..pixels as u64).map(|n| n & 0xff).fold(0u64, u64::wrapping_add))
    }
}

#[measure_all]
pub mod index {
    use super::*;

    pub fn record(id: u64) -> u64 {
        tokenize(id).wrapping_add(insert(id))
    }

    pub fn tokenize(id: u64) -> u64 {
        normalize(id).wrapping_add(black_box(id % 7))
    }

    pub fn normalize(id: u64) -> u64 {
        black_box(id.wrapping_mul(2_654_435_761) >> 8)
    }

    pub fn insert(id: u64) -> u64 {
        black_box(id ^ 0x9e37)
    }
}

/// One photo through every stage. The fan-out a flame graph draws.
#[lightwatch::measure]
pub fn one_photo(id: u64, pixels: usize) -> u64 {
    let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
    scan::header(&bytes)
        .wrapping_add(decode::frame(pixels))
        .wrapping_add(resize::thumbnail(pixels))
        .wrapping_add(encode::thumbnail(pixels / 4))
        .wrapping_add(index::record(id))
}

#[lightwatch::measure]
pub fn batch(photos: u64, pixels: usize) -> u64 {
    (0..photos).map(|id| one_photo(id, pixels)).fold(0u64, u64::wrapping_add)
}

#[lightwatch::measure]
pub fn run(photos: u64, pixels: usize) -> u64 {
    batch(photos, pixels)
}
