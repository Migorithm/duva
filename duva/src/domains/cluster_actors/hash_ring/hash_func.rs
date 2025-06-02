use std::num::Wrapping;

#[inline]
pub(crate) fn fnv_1a_hash(value: &str) -> u64 {
    // Using FNV-1a hash algorithm which is:
    // - Fast
    // - Good distribution
    // - Deterministic
    const FNV_PRIME: u64 = 1099511628211;
    const FNV_OFFSET_BASIS: u64 = 14695981039346656037;

    let mut hash = Wrapping(FNV_OFFSET_BASIS);

    for byte in value.bytes() {
        hash ^= Wrapping(byte as u64);
        hash *= Wrapping(FNV_PRIME);
    }

    // Final mixing steps (inspired by MurmurHash3 finalizer)
    let mut h = hash.0;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;

    h
}

#[test]
fn test_hash_deterministic() {
    // Same input should always produce same output
    assert_eq!(fnv_1a_hash("a"), fnv_1a_hash("a"));
    assert_eq!(fnv_1a_hash("z"), fnv_1a_hash("z"));
    assert_eq!(fnv_1a_hash("test_key"), fnv_1a_hash("test_key"));
}

#[test]
fn test_hash64_uniqueness() {
    let hashes: Vec<u64> = (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();
    assert_eq!(hashes.iter().copied().count(), 26, "Expected all hashes to be unique");
}

#[test]
fn test_hash64_range_spread() {
    let hashes: Vec<u64> = (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();

    let min = *hashes.iter().min().unwrap();
    let max = *hashes.iter().max().unwrap();
    let span = max - min;

    assert!(
        span > u64::MAX / 16,
        "Hash range is too narrow: {span} (expected > {})",
        u64::MAX / 16
    );
}

#[test]
fn test_hash64_bit_entropy() {
    // To check that the output values of hash function use a wide spread of bits
    // Every bit in the output should have a chance to flip based on different inputs.
    // If some bits are always 0, it means the hash values are not using the full 64-bit space, which reduces entropy and makes collisions more likely.
    // This is especially bad in consistent hashing, where you're relying on even, high-entropy distribution around a hash ring.

    let hashes: Vec<u64> = (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();

    let mut bit_union = 0u64;
    for &h in &hashes {
        //merges the set bits of all hash outputs.
        //At the end, bit_union is a single u64 value where each 1 bit means at least one of the hashes had that bit set.
        bit_union |= h;
    }

    let bit_count = bit_union.count_ones();
    assert!(bit_count >= 48, "Expected at least 48 bits of entropy, got only {bit_count}");
}

#[test]
fn test_hash64_average_dispersion() {
    let mut hashes: Vec<u64> =
        (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();

    hashes.sort_unstable();

    let span = hashes.last().unwrap() - hashes.first().unwrap();
    let mut gaps = Vec::with_capacity(hashes.len() - 1);
    for i in 1..hashes.len() {
        gaps.push(hashes[i] - hashes[i - 1]);
    }

    let avg_gap = gaps.iter().sum::<u64>() as f64 / gaps.len() as f64;
    let ideal_gap = span as f64 / (hashes.len() - 1) as f64;

    let lower = ideal_gap * 0.5;
    let upper = ideal_gap * 1.5;

    assert!(
        (avg_gap >= lower) && (avg_gap <= upper),
        "Average hash gap is too uneven: avg = {avg_gap:.2}, expected ~{ideal_gap:.2}"
    );
}

#[test]
fn test_hash_collision_resistance() {
    // Test that similar inputs produce different outputs
    let hash1 = fnv_1a_hash("test1");
    let hash2 = fnv_1a_hash("test2");
    let hash3 = fnv_1a_hash("test3");

    assert_ne!(hash1, hash2);
    assert_ne!(hash2, hash3);
    assert_ne!(hash1, hash3);
}

#[test]
fn test_hash_avalanche_effect() {
    // Test that small changes in input produce large changes in output
    let hash1 = fnv_1a_hash("test");
    let hash2 = fnv_1a_hash("test ");
    let hash3 = fnv_1a_hash("test1");

    // Calculate hamming distance between hashes
    let hamming_distance = |a: u64, b: u64| (a ^ b).count_ones();

    // The hamming distance should be significant (at least 8 bits different)
    assert!(
        hamming_distance(hash1, hash2) >= 8,
        "Small changes should cause significant hash changes"
    );
    assert!(
        hamming_distance(hash1, hash3) >= 8,
        "Small changes should cause significant hash changes"
    );
    assert!(
        hamming_distance(hash2, hash3) >= 8,
        "Small changes should cause significant hash changes"
    );
}
