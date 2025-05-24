use crate::prelude::PeerIdentifier;
use std::collections::{BTreeMap, HashSet};
use std::num::Wrapping;
use std::ops::Range;
use std::rc::Rc;
/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.

// Number of virtual nodes to create for each physical node.
const V_NODE_NUM: u16 = 256;

#[derive(Debug, Default)]
pub struct HashRing {
    vnodes: BTreeMap<u64, Rc<PeerIdentifier>>,
    pnodes: HashSet<Rc<PeerIdentifier>>,
}

// ! SAFETY: HashRing is supposed to be used in a single-threaded context
// ! with cluster actor as the actor is the access point to the ring.
unsafe impl Send for HashRing {}

impl HashRing {
    pub fn add_node(&mut self, peer_id: PeerIdentifier) {
        let rc_peer = Rc::new(peer_id.clone());
        // Create virtual nodes for better distribution
        for i in 0..V_NODE_NUM {
            let virtual_node_id = format!("{}-{}", peer_id, i);
            let hash = fnv_1a_hash(&virtual_node_id);

            self.vnodes.insert(hash, rc_peer.clone());
        }

        // Update physical node mapping
        self.pnodes.insert(rc_peer);
    }

    pub fn remove_node(&mut self, pnode_id: &PeerIdentifier) {
        // Remove all virtual nodes for this physical node
        self.vnodes.retain(|_, peer_id| peer_id.as_ref() != pnode_id);

        // Remove the physical node
        self.pnodes.remove(pnode_id);
    }

    pub fn get_node_for_key(&self, key: &str) -> Option<&PeerIdentifier> {
        let hash = fnv_1a_hash(key);

        // * Find the first virtual node that's greater than or equal to the key's hash
        self.vnodes
            .range(hash..)
            .next()
            .or_else(|| self.vnodes.first_key_value())
            .map(|(_, peer_id)| peer_id.as_ref())
    }

    /// Returns the token ranges that a specific node covers in the hash ring.
    /// Each range is represented as (start_hash, end_hash), where the node is responsible
    /// for all tokens >= start_hash and < end_hash.
    pub fn get_token_ranges_for_node(&self, node_id: &PeerIdentifier) -> Vec<Range<u64>> {
        // If node doesn't exist or the ring is empty, return empty vector
        if !self.pnodes.contains(node_id) || self.vnodes.is_empty() {
            return Vec::new();
        }

        // Get all vnodes in order
        let vnodes: Vec<(&u64, &std::rc::Rc<PeerIdentifier>)> = self.vnodes.iter().collect();
        let mut ranges = Vec::<Range<u64>>::new();

        // Find ranges where this node is responsible
        for i in 0..vnodes.len() {
            let current_hash = *vnodes[i].0;
            let current_node = vnodes[i].1;

            // Skip if this vnode doesn't belong to our target node
            if current_node.as_ref() != node_id {
                continue;
            }

            // Calculate the previous hash (which is the start of this range)
            // If this is the first entry, use the last entry's hash
            let prev_idx = if i == 0 { vnodes.len() - 1 } else { i - 1 };
            let start_hash = *vnodes[prev_idx].0 + 1; // Start just after previous node's hash

            // The end of the range is this node's hash
            let end_hash = current_hash + 1; // +1 because ranges are exclusive at the high end

            // * Handling wrap-around case
            if start_hash <= end_hash {
                ranges.push(start_hash..end_hash);
            } else {
                // Split into two ranges: from start to max u64, and from 0 to end
                ranges.push(start_hash..u64::MAX);
                ranges.push(0..end_hash);
            }
        }

        // Merge contiguous ranges
        ranges.sort_by_key(|r| r.start);
        let mut merged_ranges = Vec::<Range<u64>>::new();

        for range in ranges {
            if let Some(last) = merged_ranges.last_mut() {
                // If this range starts right after the previous one ends
                if last.end == range.start {
                    let new_range = last.start..range.end;
                    *last = new_range;
                    continue;
                }
            }
            merged_ranges.push(range);
        }

        merged_ranges
    }

    #[cfg(test)]
    fn get_virtual_nodes(&self) -> Vec<(&u64, &std::rc::Rc<PeerIdentifier>)> {
        self.vnodes.iter().collect()
    }

    #[cfg(test)]
    fn get_pnode_count(&self) -> usize {
        self.pnodes.len()
    }

    #[cfg(test)]
    fn get_vnode_count(&self) -> usize {
        self.vnodes.len()
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_hash_deterministic() {
        // Same input should always produce same output
        assert_eq!(fnv_1a_hash("a"), fnv_1a_hash("a"));
        assert_eq!(fnv_1a_hash("z"), fnv_1a_hash("z"));
        assert_eq!(fnv_1a_hash("test_key"), fnv_1a_hash("test_key"));
    }

    #[test]
    fn test_hash64_uniqueness() {
        let hashes: Vec<u64> =
            (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();

        let unique: HashSet<_> = hashes.iter().copied().collect();
        assert_eq!(unique.len(), 26, "Expected all hashes to be unique");
    }

    #[test]
    fn test_hash64_range_spread() {
        let hashes: Vec<u64> =
            (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();

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

        let hashes: Vec<u64> =
            (b'a'..=b'z').map(|c| fnv_1a_hash(&(c as char).to_string())).collect();

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

    #[test]
    fn test_add_and_remove_node() {
        let mut ring = HashRing::default();
        let node = PeerIdentifier("127.0.0.1:6379".into());

        ring.add_node(node.clone());
        assert_eq!(ring.get_pnode_count(), 1);
        assert_eq!(ring.get_vnode_count(), 3);

        ring.remove_node(&node);
        assert_eq!(ring.get_pnode_count(), 0);
        assert_eq!(ring.get_vnode_count(), 0);
    }

    #[test]
    fn test_get_node_for_key() {
        let mut ring = HashRing::default();
        let node = PeerIdentifier("127.0.0.1:6379".into());
        ring.add_node(node.clone());

        let key = "test_key";
        let node = ring.get_node_for_key(key);
        assert!(node.is_some());
    }

    #[test]
    fn test_multiple_nodes() {
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6379".into());
        let node2 = PeerIdentifier("127.0.0.1:6380".into());

        ring.add_node(node1);
        ring.add_node(node2);

        assert_eq!(ring.get_pnode_count(), 2);
        assert_eq!(ring.get_vnode_count(), 6);
    }

    #[test]
    fn test_consistent_hashing() {
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6379".into());
        let node2 = PeerIdentifier("127.0.0.1:6380".into());
        let node3 = PeerIdentifier("127.0.0.1:6389".into());

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);

        let key = "test_key";
        let node_got1 = ring.get_node_for_key(key);
        let node_got2 = ring.get_node_for_key(key);

        assert_eq!(node_got1, node_got2);
    }

    #[test]
    fn test_node_removal_redistribution() {
        // GIVEN: Create a hash ring with 3 nodes
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6379".into());
        let node2 = PeerIdentifier("127.0.0.1:6380".into());
        let node3 = PeerIdentifier("127.0.0.1:6381".into());

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);

        // Record initial key distribution
        let mut before_removal = Vec::new();
        for i in 0..100 {
            let key = format!("key{}", i);
            if let Some(node) = ring.get_node_for_key(&key) {
                before_removal.push((key, node.clone()));
            }
        }

        // WHEN Remove one node
        ring.remove_node(&"127.0.0.1:6379".to_string().into());

        // keys are accessed again
        let mut redistributed = 0;
        for (key, old_addr) in before_removal {
            if let Some(new_node) = ring.get_node_for_key(&key) {
                if *new_node != old_addr {
                    redistributed += 1;
                }
            }
        }

        //THEN
        assert!(redistributed > 0); // Some keys must be redistributed
        assert!(redistributed < 100); // But not all keys should be redistributed
    }

    #[test]
    fn test_virtual_node_consistency() {
        let mut ring = HashRing::default();
        let node = PeerIdentifier("127.0.0.1:6379".into());

        ring.add_node(node.clone());

        let virtual_nodes = ring.get_virtual_nodes();
        assert_eq!(virtual_nodes.len(), 3);

        // remove duplicate
        let physical_nodes: HashSet<&PeerIdentifier> =
            virtual_nodes.iter().map(|(_, peer_id)| peer_id.as_ref()).collect();
        assert_eq!(physical_nodes.len(), 1);
        assert!(physical_nodes.contains::<PeerIdentifier>(&node));
    }

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::default();
        assert_eq!(ring.get_pnode_count(), 0);
        assert_eq!(ring.get_vnode_count(), 0);
        assert!(ring.get_node_for_key("test").is_none());
    }

    #[test]
    fn test_get_token_ranges_nonexistent_node() {
        let mut ring = HashRing::default();

        ring.add_node(PeerIdentifier("127.0.0.1:6349".into()));

        // Try to get ranges for a node that doesn't exist
        let ranges = ring.get_token_ranges_for_node(&"127.0.0.1:7777".to_string().into());
        assert_eq!(ranges.len(), 0);
    }

    #[test]
    fn test_get_token_ranges_single_node() {
        let mut ring = HashRing::default();
        let node_id = "127.0.0.1:6349";
        ring.add_node(PeerIdentifier(node_id.into()));

        let ranges = ring.get_token_ranges_for_node(&node_id.to_string().into());

        // A single node should own the entire hash space
        // But might be split into multiple ranges because of virtual nodes
        let mut total_coverage: u64 = 0;
        calculate_total_coverage(&ranges, &mut total_coverage);

        // Should own the entire hash space
        assert_eq!(total_coverage, u64::MAX);
    }

    #[test]
    fn test_get_token_ranges_multiple_nodes() {
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6349".to_string());
        let node2 = PeerIdentifier("127.0.0.1:6350".to_string());

        ring.add_node(node1.clone());
        ring.add_node(node2.clone());

        let ranges1 = ring.get_token_ranges_for_node(&node1);
        let ranges2 = ring.get_token_ranges_for_node(&node2);

        // Verify ranges are non-empty
        assert!(!ranges1.is_empty());
        assert!(!ranges2.is_empty());

        // Calculate total coverage
        let mut total_coverage1: u64 = 0;
        let mut total_coverage2: u64 = 0;

        calculate_total_coverage(&ranges1, &mut total_coverage1);
        calculate_total_coverage(&ranges2, &mut total_coverage2);

        // Combined coverage should be the entire hash space
        assert_eq!(total_coverage1 + total_coverage2, u64::MAX);

        // Verify no overlapping ranges
        for range1 in &ranges1 {
            for range2 in &ranges2 {
                // Check if ranges overlap
                assert!(
                    !ranges_overlap(range1, range2),
                    "Ranges overlap: {:?} and {:?}",
                    range1,
                    range2
                );
            }
        }
    }

    #[test]
    fn test_ranges_after_node_removal() {
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6349".to_string());
        let node2 = PeerIdentifier("127.0.0.1:6350".to_string());
        let node3 = PeerIdentifier("127.0.0.1:6351".to_string());

        // Add three nodes
        ring.add_node(node1.clone());
        ring.add_node(node2.clone());
        ring.add_node(node3.clone());

        // Get initial ranges
        let initial_ranges1 = ring.get_token_ranges_for_node(&node1);
        let initial_ranges2 = ring.get_token_ranges_for_node(&node2);

        // Remove node3
        ring.remove_node(&node3);

        // Get updated ranges
        let updated_ranges1 = ring.get_token_ranges_for_node(&node1.into());
        let updated_ranges2 = ring.get_token_ranges_for_node(&node2.into());

        // Verify node3 has no ranges
        let ranges3 = ring.get_token_ranges_for_node(&node3.into());
        assert_eq!(ranges3.len(), 0);

        // The remaining nodes should split the entire hash space
        let mut total_coverage: u64 = 0;

        calculate_total_coverage(&updated_ranges1, &mut total_coverage);
        calculate_total_coverage(&updated_ranges2, &mut total_coverage);

        // Should own the entire hash space
        assert_eq!(total_coverage, u64::MAX);

        // Verify the ranges have changed (this is expected as removing a node
        // should redistribute the hash space)
        assert_ne!(initial_ranges1, updated_ranges1);
        assert_ne!(initial_ranges2, updated_ranges2);
    }

    // Helper function to check if two ranges overlap
    fn ranges_overlap(range1: &Range<u64>, range2: &Range<u64>) -> bool {
        let (start1, end1) = (range1.start, range1.end);
        let (start2, end2) = (range2.start, range2.end);

        // Handle normal ranges
        if start1 < end1 && start2 < end2 {
            return start1 < end2 && end1 > start2;
        }

        // Handle wrap-around ranges
        if start1 >= end1 {
            return start2 < end1 || end2 > start1;
        }

        if start2 >= end2 {
            return start1 < end2 || end1 > start2;
        }

        false
    }

    fn calculate_total_coverage(ranges: &Vec<Range<u64>>, total_coverage: &mut u64) {
        for range in ranges {
            if range.end > range.start {
                *total_coverage += range.end - range.start;
            } else {
                // Handle wrap-around case
                *total_coverage += (u64::MAX - range.start + 1) + range.end;
            }
        }
    }
}
