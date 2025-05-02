use crate::domains::peers::peer::PeerState;
use crate::prelude::PeerIdentifier;
use std::collections::{BTreeMap, HashMap};
use std::num::Wrapping;

/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.
///
#[derive(Debug)]
pub struct HashRing {
    vnodes: BTreeMap<u64, PeerIdentifier>,
    pnodes: HashMap<PeerIdentifier, PeerState>,
    vnode_num: usize, // Number of virtual nodes to create for each physical node.
}

impl HashRing {
    pub fn new(vnode_num: usize) -> Self {
        Self { vnodes: BTreeMap::new(), pnodes: HashMap::new(), vnode_num }
    }

    pub fn add_node(&mut self, peer_state: PeerState) {
        let pnode_id = peer_state.addr.clone();

        // Create virtual nodes for better distribution
        for i in 0..self.vnode_num {
            let virtual_node_id = format!("{}-{}", pnode_id, i);
            let hash = fnv_1a_hash(&virtual_node_id);

            self.vnodes.insert(hash, pnode_id.clone());
        }

        // Update physical node mapping
        self.pnodes.insert(pnode_id, peer_state);
    }

    pub fn remove_node(&mut self, pnode_id: &PeerIdentifier) {
        // Remove all virtual nodes for this physical node
        self.vnodes.retain(|_, peer_id| peer_id != pnode_id);

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
            .map(|(_, peer_id)| peer_id)
    }

    #[cfg(test)]
    fn get_virtual_nodes(&self) -> Vec<(&u64, &PeerIdentifier)> {
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
fn fnv_1a_hash(value: &str) -> u64 {
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
    use std::collections::HashSet;

    use super::*;
    use crate::domains::cluster_actors::replication::ReplicationId;
    use crate::domains::peers::peer::NodeKind;

    fn create_test_node(addr: &str) -> PeerState {
        PeerState::new(addr, 0, ReplicationId::Key("test".to_string()), NodeKind::Replica)
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
        let mut ring = HashRing::new(3);
        let node = create_test_node("127.0.0.1:6379");
        let node_id = node.addr.clone();

        ring.add_node(node);
        assert_eq!(ring.get_pnode_count(), 1);
        assert_eq!(ring.get_vnode_count(), 3);

        ring.remove_node(&node_id);
        assert_eq!(ring.get_pnode_count(), 0);
        assert_eq!(ring.get_vnode_count(), 0);
    }

    #[test]
    fn test_get_node_for_key() {
        let mut ring = HashRing::new(3);
        let node = create_test_node("127.0.0.1:6379");
        ring.add_node(node);

        let key = "test_key";
        let node = ring.get_node_for_key(key);
        assert!(node.is_some());
    }

    #[test]
    fn test_multiple_nodes() {
        let mut ring = HashRing::new(3);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");

        ring.add_node(node1);
        ring.add_node(node2);

        assert_eq!(ring.get_pnode_count(), 2);
        assert_eq!(ring.get_vnode_count(), 6);
    }

    #[test]
    fn test_consistent_hashing() {
        let mut ring = HashRing::new(256);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");
        let node3 = create_test_node("127.0.0.1:6389");

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
        let mut ring = HashRing::new(3);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");
        let node3 = create_test_node("127.0.0.1:6381");

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
        let mut ring = HashRing::new(3);
        let node = create_test_node("127.0.0.1:6379");
        let node_id = node.addr.clone();
        ring.add_node(node);

        let virtual_nodes = ring.get_virtual_nodes();
        assert_eq!(virtual_nodes.len(), 3);

        // remove duplicate
        let physical_nodes: HashSet<_> =
            virtual_nodes.iter().map(|(_, peer_id)| *peer_id).collect();
        assert_eq!(physical_nodes.len(), 1);
        assert!(physical_nodes.contains(&node_id));
    }

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::new(3);
        assert_eq!(ring.get_pnode_count(), 0);
        assert_eq!(ring.get_vnode_count(), 0);
        assert!(ring.get_node_for_key("test").is_none());
    }
}
