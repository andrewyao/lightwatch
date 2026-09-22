//! A hasher for the integer keys on the measurement hot path.
//!
//! Every map the probe touches per call is keyed by an interned id: a function
//! id, a calling-context id, a bucket index, or a pair of those. The default
//! `SipHash` costs more to hash one `u32` than the rest of the lookup costs
//! together, and its collision resistance buys nothing here because the keys
//! are counters we handed out ourselves, not input an adversary chooses.
//!
//! So: multiply by the golden-ratio constant, which scatters sequential ids
//! across the whole word, then fold the high half down. `hashbrown` reads the
//! top seven bits for its control byte and the low bits for the bucket index,
//! so entropy has to reach both ends.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// 2^64 divided by the golden ratio. Odd, so multiplying by it is invertible
/// and no two distinct ids collapse onto each other.
const SCATTER: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Default)]
pub(crate) struct IntHasher(u64);

impl Hasher for IntHasher {
    fn finish(&self) -> u64 {
        self.0 ^ (self.0 >> 32)
    }

    /// Only reached by a key type that hashes as bytes. Nothing on the hot
    /// path does, but `Hasher` requires it and a wrong answer here would be a
    /// silent collision rather than a compile error.
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.mix(*byte as u64);
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.mix(value as u64);
    }

    fn write_u16(&mut self, value: u16) {
        self.mix(value as u64);
    }

    fn write_u32(&mut self, value: u32) {
        self.mix(value as u64);
    }

    fn write_u64(&mut self, value: u64) {
        self.mix(value);
    }

    fn write_usize(&mut self, value: usize) {
        self.mix(value as u64);
    }
}

impl IntHasher {
    /// Written so that a tuple key, which arrives as two `write_u32` calls,
    /// depends on the order of its fields: `(1, 2)` and `(2, 1)` are different
    /// calling contexts and must not share a slot.
    fn mix(&mut self, value: u64) {
        self.0 = (self.0 ^ value).wrapping_mul(SCATTER);
    }
}

/// A `HashMap` keyed by ids the probe interned itself.
pub(crate) type IntMap<K, V> = HashMap<K, V, BuildHasherDefault<IntHasher>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hash;

    fn hash_of<T: Hash>(value: T) -> u64 {
        let mut hasher = IntHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn a_pair_depends_on_the_order_of_its_halves() {
        assert_ne!(hash_of((1u32, 2u32)), hash_of((2u32, 1u32)));
    }

    #[test]
    fn sequential_ids_do_not_land_in_one_bucket() {
        // The ids the probe hands out are 1, 2, 3, .... A hash that leaves
        // them adjacent in the low bits turns every map into a linear scan.
        let low_bits: std::collections::HashSet<u64> =
            (1u32..=64).map(|id| hash_of(id) & 63).collect();
        assert!(
            low_bits.len() > 40,
            "64 consecutive ids spread over only {} of 64 buckets",
            low_bits.len()
        );
    }

    #[test]
    fn the_map_still_behaves_like_a_map() {
        let mut map: IntMap<(u32, u32), u64> = IntMap::default();
        for from in 0..100u32 {
            for to in 0..10u32 {
                map.insert((from, to), (from * 10 + to) as u64);
            }
        }
        assert_eq!(map.len(), 1000);
        assert_eq!(map.get(&(37, 4)), Some(&374));
        assert_eq!(map.get(&(4, 37)), None);
    }
}
