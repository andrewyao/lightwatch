//! Log-linear bucketing, shared by every emitter and the daemon.
//!
//! Buckets are exact for values under 32 and carry 16 sub-buckets per octave
//! above it, so a bucketed value is never more than 6.25% from the truth. A
//! client in another language reproduces this from `bucket_of` alone, or skips
//! it entirely and sends [`Dist::Raw`].

use serde::{Deserialize, Serialize};

/// Values below this are stored exactly, one bucket each.
pub const LINEAR_LIMIT: u64 = 32;

/// Sub-buckets per octave above [`LINEAR_LIMIT`]. 16 gives <= 6.25% error.
pub const SUB_BUCKETS: u64 = 16;

const SUB_BITS: u32 = 4;

/// Largest index [`bucket_of`] can return, for sizing a dense array.
pub const MAX_BUCKET: u16 = 975;

/// Raw samples above this in one window must be bucketed by the client.
pub const MAX_RAW_SAMPLES: usize = 1024;

/// The bucket holding `v`.
pub fn bucket_of(v: u64) -> u16 {
    if v < LINEAR_LIMIT {
        return v as u16;
    }
    let octave = 63 - v.leading_zeros() as u64;
    let sub = (v >> (octave - SUB_BITS as u64)) - SUB_BUCKETS;
    (LINEAR_LIMIT + (octave - 5) * SUB_BUCKETS + sub) as u16
}

/// Inclusive value range a bucket covers.
pub fn bucket_range(index: u16) -> (u64, u64) {
    let i = index as u64;
    if i < LINEAR_LIMIT {
        return (i, i);
    }
    let j = i - LINEAR_LIMIT;
    let octave = 5 + j / SUB_BUCKETS;
    let sub = j % SUB_BUCKETS;
    let shift = octave - SUB_BITS as u64;
    let lo = (SUB_BUCKETS + sub) << shift;
    (lo, lo + ((1 << shift) - 1))
}

/// A distribution on the wire.
///
/// `Raw` exists so a client in any language can emit a distribution without
/// porting [`bucket_of`]. The daemon buckets it on arrival, so the two are
/// interchangeable once ingested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "enc", rename_all = "snake_case")]
pub enum Dist {
    Raw { v: Vec<u64> },
    Buckets { b: Vec<(u16, u32)> },
}

impl Dist {
    pub fn empty() -> Self {
        Dist::Buckets { b: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Dist::Raw { v } => v.is_empty(),
            Dist::Buckets { b } => b.is_empty(),
        }
    }

    /// Total number of samples the distribution stands for.
    pub fn count(&self) -> u64 {
        match self {
            Dist::Raw { v } => v.len() as u64,
            Dist::Buckets { b } => b.iter().map(|(_, c)| *c as u64).sum(),
        }
    }

    /// Collapses to sparse buckets, sorted by index. Already-bucketed input is
    /// re-folded so duplicate indices from a sloppy client merge rather than
    /// double-count.
    pub fn to_buckets(&self) -> Vec<(u16, u32)> {
        let mut counts: Vec<(u16, u32)> = match self {
            Dist::Raw { v } => {
                let mut c: Vec<(u16, u32)> = Vec::new();
                for value in v {
                    c.push((bucket_of(*value), 1));
                }
                c
            }
            Dist::Buckets { b } => b.clone(),
        };
        counts.sort_unstable_by_key(|(i, _)| *i);
        let mut folded: Vec<(u16, u32)> = Vec::with_capacity(counts.len());
        for (index, count) in counts {
            match folded.last_mut() {
                Some((last, running)) if *last == index => *running += count,
                _ => folded.push((index, count)),
            }
        }
        folded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_lands_in_a_bucket_that_contains_it() {
        let mut v = 0u64;
        while v < 1 << 20 {
            let (lo, hi) = bucket_range(bucket_of(v));
            assert!(lo <= v && v <= hi, "{v} fell outside [{lo}, {hi}]");
            v = v + 1 + v / 97;
        }
    }

    #[test]
    fn buckets_are_contiguous_and_strictly_ordered() {
        let mut previous_hi = None;
        for index in 0..=MAX_BUCKET {
            let (lo, hi) = bucket_range(index);
            assert!(lo <= hi, "bucket {index} is inverted");
            if let Some(prev) = previous_hi {
                assert_eq!(lo, prev + 1, "gap or overlap before bucket {index}");
            }
            previous_hi = Some(hi);
        }
    }

    #[test]
    fn bucketing_error_stays_under_the_documented_bound() {
        for v in [32u64, 100, 1_000, 44_720_000, u32::MAX as u64] {
            let (lo, hi) = bucket_range(bucket_of(v));
            let width = (hi - lo + 1) as f64;
            assert!(width / lo as f64 <= 0.0625, "{v} exceeded 6.25%");
        }
    }

    #[test]
    fn max_bucket_is_the_real_maximum() {
        assert_eq!(bucket_of(u64::MAX), MAX_BUCKET);
    }

    #[test]
    fn raw_and_bucketed_forms_agree() {
        let raw = Dist::Raw { v: vec![5, 5, 900, 44_720_000] };
        let bucketed = Dist::Buckets { b: raw.to_buckets() };
        assert_eq!(raw.to_buckets(), bucketed.to_buckets());
        assert_eq!(raw.count(), bucketed.count());
    }

    #[test]
    fn duplicate_indices_from_a_client_merge_instead_of_double_counting() {
        let sloppy = Dist::Buckets { b: vec![(7, 2), (3, 1), (7, 5)] };
        assert_eq!(sloppy.to_buckets(), vec![(3, 1), (7, 7)]);
        assert_eq!(sloppy.count(), 8);
    }
}
