/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.
///
use crate::ReplicationId;
use crate::prelude::PeerIdentifier;
use std::collections::{BTreeMap, HashMap};
use std::num::Wrapping;
use std::rc::Rc;

// Number of virtual nodes to create for each physical node.
const V_NODE_NUM: u16 = 256;

#[derive(Debug, Default, bincode::Decode, bincode::Encode, Clone, Eq)]
pub struct HashRing {
    vnodes: BTreeMap<u64, Rc<ReplicationId>>,
    // TODO value in the following map must be replaced when election happens
    pnodes: HashMap<ReplicationId, PeerIdentifier>,
    pub(crate) last_modified: u128,
}

// ! SAFETY: HashRing is supposed to be used in a single-threaded context
// ! with cluster actor as the actor is the access point to the ring.
unsafe impl Send for HashRing {}
unsafe impl Sync for HashRing {}

impl HashRing {
    fn exists(&self, replid: &ReplicationId) -> bool {
        self.pnodes.contains_key(replid)
    }
    fn update_last_modified(&mut self) {
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
    }

    /// Adds a new partition to the hash ring if it doesn't already exist.
    pub fn add_partition_if_not_exists(
        &mut self,
        repl_id: ReplicationId,
        leader_id: PeerIdentifier,
    ) -> Option<()> {
        if self.exists(&repl_id) {
            return None;
        }

        self.pnodes.insert(repl_id.clone(), leader_id);

        let repl_id = Rc::new(repl_id.clone());
        // Create virtual nodes for better distribution
        for i in 0..V_NODE_NUM {
            let virtual_node_id = format!("{}-{}", repl_id, i);
            let hash = fnv_1a_hash(&virtual_node_id);

            self.vnodes.insert(hash, repl_id.clone());
        }

        self.update_last_modified();

        Some(())
    }

    /// The following method will be invoked when:
    /// - ClusterForget command is received
    /// - node is identified as dead/idle
    pub(super) fn remove_partition(&mut self, target_repl_id: &ReplicationId) {
        // Remove all virtual nodes for this physical node
        self.vnodes.retain(|_, repl_id| repl_id.as_ref() != target_repl_id);

        // Remove the physical node
        self.pnodes.remove(target_repl_id);

        self.update_last_modified();
    }

    fn find_node(&self, hash: u64) -> Option<&ReplicationId> {
        // Find the first vnode with hash >= target hash
        self.vnodes
            .range(hash..)
            .next()
            .or_else(|| self.vnodes.iter().next()) // wrap around to first node
            .map(|(_, node_id)| node_id.as_ref())
    }

    pub(crate) async fn create_migration_tasks(
        &self,
        new_ring: &HashRing,
        keys: Vec<String>,
    ) -> Vec<MigrationTask> {
        let mut migration_tasks = Vec::new();

        // Get all token positions from both rings as partition boundaries
        let mut tokens: Vec<u64> =
            self.vnodes.keys().chain(new_ring.vnodes.keys()).cloned().collect();
        tokens.sort();
        tokens.dedup();

        // Check each partition for ownership changes
        for (i, &token) in tokens.iter().enumerate() {
            let prev_token = if i == 0 { tokens[tokens.len() - 1] } else { tokens[i - 1] };
            let (start, end) = (prev_token.wrapping_add(1), token);

            if let (Some(old_owner), Some(new_owner)) =
                (self.find_node(token), new_ring.find_node(token))
            {
                // If both old and new owners exist, we need to check if ownership changed
                if old_owner != new_owner {
                    // Node ownership changed for this partition
                    // Need to migrate data from old node to new node
                    let affected_keys = filter_keys_in_partition(&keys, start, end);
                    if !affected_keys.is_empty() {
                        migration_tasks.push(MigrationTask {
                            partition_range: (start, end),
                            from_node: old_owner.clone(),
                            to_node: new_owner.clone(),
                            keys_to_migrate: affected_keys,
                        });
                    }
                }
            }
        }
        migration_tasks
    }

    #[cfg(test)]
    pub(crate) fn get_virtual_nodes(&self) -> Vec<(&u64, &std::rc::Rc<ReplicationId>)> {
        self.vnodes.iter().collect()
    }

    #[cfg(test)]
    pub(crate) fn get_pnode_count(&self) -> usize {
        self.pnodes.len()
    }

    #[cfg(test)]
    pub(crate) fn get_vnode_count(&self) -> usize {
        self.vnodes.len()
    }

    #[cfg(test)]
    fn get_node_for_key(&self, key: &str) -> Option<&ReplicationId> {
        let hash = fnv_1a_hash(key);
        self.find_node(hash)
    }
}

fn filter_keys_in_partition(
    keys: &[String],
    partition_start: u64,
    partition_end: u64,
) -> Vec<String> {
    keys.iter()
        .filter(|key| {
            let key_hash = fnv_1a_hash(key);
            // Check if key hash falls in range (partition_start, partition_end]
            // Handle wrap-around case where start > end
            if partition_start < partition_end {
                key_hash > partition_start && key_hash <= partition_end
            } else {
                // Wrap-around: key is either > start OR <= end
                key_hash > partition_start || key_hash <= partition_end
            }
        })
        .cloned()
        .collect()
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

#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub partition_range: (u64, u64), // (start_hash, end_hash)
    pub from_node: ReplicationId,
    pub to_node: ReplicationId,
    pub keys_to_migrate: Vec<String>, // actual keys in this range
}

impl PartialEq for HashRing {
    fn eq(&self, other: &Self) -> bool {
        self.vnodes == other.vnodes && self.pnodes == other.pnodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, thread::sleep, time::Duration};

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
        let modified_time = ring.last_modified;
        let node = PeerIdentifier("127.0.0.1:6379".into());
        let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());

        ring.add_partition_if_not_exists(repl_id.clone(), node.clone());
        let modified_time_after_add = ring.last_modified;
        assert_eq!(ring.get_pnode_count(), 1);
        assert_eq!(ring.get_vnode_count(), 256);
        assert!(modified_time < modified_time_after_add);

        sleep(Duration::from_millis(1)); // Ensure time has changed
        ring.remove_partition(&repl_id);
        assert_eq!(ring.get_pnode_count(), 0);
        assert_eq!(ring.get_vnode_count(), 0);
        assert!(ring.last_modified > modified_time_after_add);
    }

    #[test]
    fn test_get_node_for_key() {
        let mut ring = HashRing::default();
        let node = PeerIdentifier("127.0.0.1:6379".into());
        let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
        ring.add_partition_if_not_exists(repl_id.clone(), node.clone());

        let key = "test_key";
        let node = ring.get_node_for_key(key);
        assert!(node.is_some());
    }

    #[test]
    fn test_multiple_nodes() {
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6379".into());
        let repl_id1 = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
        let node2 = PeerIdentifier("127.0.0.1:6380".into());
        let repl_id2 = ReplicationId::Key(uuid::Uuid::now_v7().to_string());

        ring.add_partition_if_not_exists(repl_id1, node1);
        ring.add_partition_if_not_exists(repl_id2, node2);

        assert_eq!(ring.get_pnode_count(), 2);
        assert_eq!(ring.get_vnode_count(), 512);
    }

    #[test]
    fn test_consistent_hashing() {
        let mut ring = HashRing::default();
        let node1 = PeerIdentifier("127.0.0.1:6379".into());
        let node2 = PeerIdentifier("127.0.0.1:6380".into());
        let node3 = PeerIdentifier("127.0.0.1:6389".into());

        ring.add_partition_if_not_exists(
            ReplicationId::Key(uuid::Uuid::now_v7().to_string()),
            node1,
        );
        ring.add_partition_if_not_exists(
            ReplicationId::Key(uuid::Uuid::now_v7().to_string()),
            node2,
        );
        ring.add_partition_if_not_exists(
            ReplicationId::Key(uuid::Uuid::now_v7().to_string()),
            node3,
        );

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

        let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
        ring.add_partition_if_not_exists(repl_id.clone(), node1);
        ring.add_partition_if_not_exists(
            ReplicationId::Key(uuid::Uuid::now_v7().to_string()),
            node2,
        );
        ring.add_partition_if_not_exists(
            ReplicationId::Key(uuid::Uuid::now_v7().to_string()),
            node3,
        );

        // Record initial key distribution
        let mut before_removal = Vec::new();
        for i in 0..100 {
            let key = format!("key{}", i);
            if let Some(node) = ring.get_node_for_key(&key) {
                before_removal.push((key, node.clone()));
            }
        }

        // WHEN Remove one node
        ring.remove_partition(&repl_id);

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

        let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
        ring.add_partition_if_not_exists(repl_id.clone(), node.clone());

        let virtual_nodes = ring.get_virtual_nodes();
        assert_eq!(virtual_nodes.len(), 256);

        // remove duplicate
        let physical_nodes: HashSet<&ReplicationId> =
            virtual_nodes.iter().map(|(_, peer_id)| peer_id.as_ref()).collect();
        assert_eq!(physical_nodes.len(), 1);
        assert!(physical_nodes.contains::<ReplicationId>(&repl_id));
    }

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::default();
        assert_eq!(ring.get_pnode_count(), 0);
        assert_eq!(ring.get_vnode_count(), 0);
        assert!(ring.get_node_for_key("test").is_none());
    }

    #[test]
    fn test_eq_works_deterministically() {
        let mut ring = HashRing::default();
        let repl_id = ReplicationId::Key("dsdsdds".to_string());
        let node = PeerIdentifier("127.0.0.1:3499".to_string());
        ring.add_partition_if_not_exists(repl_id.clone(), node.clone());

        let mut ring_to_compare = HashRing::default();
        ring_to_compare.add_partition_if_not_exists(repl_id.clone(), node.clone());
        assert_eq!(ring, ring_to_compare);

        ring.remove_partition(&repl_id);
        assert_ne!(ring, ring_to_compare);

        ring_to_compare.remove_partition(&repl_id);
        assert_eq!(ring, ring_to_compare);
    }

    #[test]
    fn test_idempotent_addition() {
        let mut ring = HashRing::default();
        let repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
        let node = PeerIdentifier("127.0.0.1:3499".to_string());
        let is_added = ring.add_partition_if_not_exists(repl_id.clone(), node.clone());
        assert!(is_added.is_some());

        let ring_to_compare = ring.clone(); // Clone to avoid borrowing issues

        // Adding the same node again should not change the ring
        let is_added = ring.add_partition_if_not_exists(repl_id.clone(), node.clone());
        assert!(is_added.is_none());

        assert_eq!(ring, ring_to_compare);
    }
}

#[cfg(test)]
mod migration_tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;
    use crate::ReplicationId;
    use crate::prelude::PeerIdentifier;

    // Migration plan tests
    fn replid_create_helper(repl_id: &str) -> ReplicationId {
        ReplicationId::Key(repl_id.to_string())
    }

    #[tokio::test]
    async fn test_no_migration_when_rings_identical() {
        let mut ring = HashRing::default();
        ring.add_partition_if_not_exists(
            ReplicationId::Key(Uuid::now_v7().to_string()),
            PeerIdentifier("127.0.0.1:6379".into()),
        );

        let keys = vec!["key1".to_string(), "key2".to_string()];
        let tasks = ring.create_migration_tasks(&ring, keys).await;

        assert!(tasks.is_empty(), "Identical rings should require no migration");
    }

    #[tokio::test]
    async fn test_single_node_ownership_change() {
        // Test scenario: 3 nodes in ring, one node (node2) is replaced by node4
        // node1 and node3 remain unchanged
        let replid1 = replid_create_helper("node1");
        let replid2 = replid_create_helper("node2");
        let replid3 = replid_create_helper("node3");
        let replid4 = replid_create_helper("node4");

        // Create old ring with 3 nodes using realistic hash ring
        let mut old_ring = HashRing::default();
        old_ring.add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()));
        old_ring.add_partition_if_not_exists(replid2.clone(), PeerIdentifier("peer2".into()));
        old_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()));

        // Create new ring where node2 is replaced by node4
        let mut new_ring = HashRing::default();
        new_ring.add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()));
        new_ring.add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into())); // node2 -> node4
        new_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()));

        // Generate a set of test keys
        let test_keys: Vec<String> = (0..10000).map(|i| format!("test_key_{}", i)).collect();

        // Identify ownership in both rings
        let mut old_ownership = HashMap::new();
        let mut new_ownership = HashMap::new();

        for key in &test_keys {
            old_ownership.insert(key.clone(), old_ring.get_node_for_key(key).unwrap().clone());
            new_ownership.insert(key.clone(), new_ring.get_node_for_key(key).unwrap().clone());
        }

        // Find keys that changed ownership
        let mut expected_migrations = HashMap::<(ReplicationId, ReplicationId), Vec<String>>::new();

        for key in &test_keys {
            let (old_owner, new_owner) =
                (old_ownership.get(key).unwrap(), new_ownership.get(key).unwrap());

            if old_ownership.get(key) != new_ownership.get(key) {
                let migration_key = (old_owner.clone(), new_owner.clone());
                expected_migrations.entry(migration_key).or_default().push(key.clone());
            }
        }

        // Create migration plan
        let tasks = old_ring.create_migration_tasks(&new_ring, test_keys.clone()).await;

        // Verify that we have migration tasks if and only if there are ownership changes
        assert!(!tasks.is_empty(), "Should have migration tasks when ownership changes");

        // Collect actual migrations from tasks
        let mut actual_migrations = HashMap::<(ReplicationId, ReplicationId), Vec<String>>::new();

        for task in &tasks {
            let migration_key = (task.from_node.clone(), task.to_node.clone());
            actual_migrations
                .entry(migration_key)
                .or_default()
                .extend(task.keys_to_migrate.clone());
        }

        // Verify that actual migrations match expected migrations
        for ((from_node, to_node), expected_keys) in &expected_migrations {
            let actual_keys = actual_migrations.get(&(from_node.clone(), to_node.clone())).unwrap();
            assert_eq!(actual_keys.len(), expected_keys.len());
            let mut ak = actual_keys.clone();
            let mut ek = expected_keys.clone();
            ak.sort();
            ek.sort();
            assert_eq!(ak, ek);
        }

        // Verify no unexpected migrations
        for ((from_node, to_node), actual_keys) in &actual_migrations {
            if !expected_migrations.contains_key(&(from_node.clone(), to_node.clone())) {
                panic!(
                    "Unexpected migration from {} to {} for keys {:?}",
                    from_node, to_node, actual_keys
                );
            }
        }

        // Verify total count
        let total_expected: usize = expected_migrations.values().map(|v| v.len()).sum();
        let total_actual: usize = tasks.iter().map(|t| t.keys_to_migrate.len()).sum();
        assert_eq!(
            total_actual, total_expected,
            "Total migrated keys should match expected migrations"
        );

        // Additional verification: node2 should not exist in new ring, node4 should not exist in old ring
        for key in &test_keys {
            // In old ring, node4 shouldn't own any keys (it doesn't exist)
            if let Some(old_owner) = old_ring.get_node_for_key(key) {
                assert_ne!(*old_owner, replid4, "Node4 shouldn't exist in old ring");
            }

            // In new ring, node2 shouldn't own any keys (it was removed)
            if let Some(new_owner) = new_ring.get_node_for_key(key) {
                assert_ne!(*new_owner, replid2, "Node2 shouldn't exist in new ring");
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_ownership_changes() {
        // Test scenario: 4 nodes in ring, replace 2 nodes simultaneously
        // node1 -> node5, node2 -> node6, node3 and node4 remain unchanged
        let replid1 = replid_create_helper("node1");
        let replid2 = replid_create_helper("node2");
        let replid3 = replid_create_helper("node3");
        let replid4 = replid_create_helper("node4");
        let replid5 = replid_create_helper("node5");
        let replid6 = replid_create_helper("node6");

        // Create old ring with 4 nodes
        let mut old_ring = HashRing::default();
        old_ring.add_partition_if_not_exists(replid1.clone(), PeerIdentifier("peer1".into()));
        old_ring.add_partition_if_not_exists(replid2.clone(), PeerIdentifier("peer2".into()));
        old_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into()));
        old_ring.add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into()));

        // Create new ring where node1->node5 and node2->node6, but node3 and node4 remain
        let mut new_ring = HashRing::default();
        new_ring.add_partition_if_not_exists(replid5.clone(), PeerIdentifier("peer5".into())); // node1 -> node5
        new_ring.add_partition_if_not_exists(replid6.clone(), PeerIdentifier("peer6".into())); // node2 -> node6
        new_ring.add_partition_if_not_exists(replid3.clone(), PeerIdentifier("peer3".into())); // node3 stays
        new_ring.add_partition_if_not_exists(replid4.clone(), PeerIdentifier("peer4".into())); // node4 stays

        // Generate a large set of test keys for comprehensive testing
        let test_keys: Vec<String> = (0..5000).map(|i| format!("test_key_{}", i)).collect();

        // Identify ownership in both rings
        let mut old_ownership = HashMap::new();
        let mut new_ownership = HashMap::new();

        for key in &test_keys {
            old_ownership.insert(key.clone(), old_ring.get_node_for_key(key).unwrap().clone());
            new_ownership.insert(key.clone(), new_ring.get_node_for_key(key).unwrap().clone());
        }

        // Find keys that changed ownership
        let mut expected_migrations = HashMap::<(ReplicationId, ReplicationId), Vec<String>>::new();

        for key in &test_keys {
            let (old_owner, new_owner) =
                (old_ownership.get(key).unwrap(), new_ownership.get(key).unwrap());

            if old_ownership.get(key) != new_ownership.get(key) {
                let migration_key = (old_owner.clone(), new_owner.clone());
                expected_migrations.entry(migration_key).or_default().push(key.clone());
            }
        }

        // Create migration plan
        let tasks = old_ring.create_migration_tasks(&new_ring, test_keys.clone()).await;

        // Should have migration tasks since we replaced multiple nodes
        assert!(!tasks.is_empty(), "Should have migration tasks when multiple nodes change");

        // Collect actual migrations from tasks
        let mut actual_migrations = HashMap::<(ReplicationId, ReplicationId), Vec<String>>::new();

        for task in &tasks {
            let migration_key = (task.from_node.clone(), task.to_node.clone());
            actual_migrations
                .entry(migration_key)
                .or_default()
                .extend(task.keys_to_migrate.clone());
        }

        // Verify that actual migrations match expected migrations exactly
        for ((from_node, to_node), expected_keys) in &expected_migrations {
            let actual_keys = actual_migrations.get(&(from_node.clone(), to_node.clone())).unwrap();

            assert_eq!(actual_keys.len(), expected_keys.len(),);

            // Sort both lists and compare for exact match
            let mut actual_sorted = actual_keys.clone();
            let mut expected_sorted = expected_keys.clone();
            actual_sorted.sort();
            expected_sorted.sort();

            assert_eq!(actual_sorted, expected_sorted,);
        }

        // Verify no unexpected migrations
        for ((from_node, to_node), actual_keys) in &actual_migrations {
            assert!(expected_migrations.contains_key(&(from_node.clone(), to_node.clone())));
        }

        // Verify total count
        let total_expected: usize = expected_migrations.values().map(|v| v.len()).sum();
        let total_actual: usize = tasks.iter().map(|t| t.keys_to_migrate.len()).sum();
        assert_eq!(total_actual, total_expected,);

        // Verify that we actually have multiple different migration paths (multiple ownership changes)
        assert!(expected_migrations.len() >= 2,);

        // Additional verification: removed nodes shouldn't exist in new ring, added nodes shouldn't exist in old ring
        for key in &test_keys {
            // In old ring, new nodes shouldn't own any keys (they don't exist)
            if let Some(old_owner) = old_ring.get_node_for_key(key) {
                assert_ne!(*old_owner, replid5, "Node5 shouldn't exist in old ring");
                assert_ne!(*old_owner, replid6, "Node6 shouldn't exist in old ring");
            }

            // In new ring, removed nodes shouldn't own any keys
            if let Some(new_owner) = new_ring.get_node_for_key(key) {
                assert_ne!(*new_owner, replid1, "Node1 shouldn't exist in new ring");
                assert_ne!(*new_owner, replid2, "Node2 shouldn't exist in new ring");
            }
        }

        // Verify that both unchanged nodes (node3, node4) still exist and own some keys in the new ring
        let mut node3_key_count = 0;
        let mut node4_key_count = 0;

        for key in &test_keys {
            if let Some(new_owner) = new_ring.get_node_for_key(key) {
                if *new_owner == replid3 {
                    node3_key_count += 1;
                } else if *new_owner == replid4 {
                    node4_key_count += 1;
                }
            }
        }

        // Both unchanged nodes should still own some keys (they weren't removed)
        assert!(node3_key_count > 0, "Node3 should still own some keys in new ring");
        assert!(node4_key_count > 0, "Node4 should still own some keys in new ring");

        println!("Migration summary:");
        for ((from, to), keys) in &expected_migrations {
            println!("  {} -> {}: {} keys", from, to, keys.len());
        }
        println!("  Node3 keys in new ring: {}", node3_key_count);
        println!("  Node4 keys in new ring: {}", node4_key_count);
        println!("  Total test keys: {}", test_keys.len());
    }

    #[tokio::test]
    async fn test_empty_keys_migration_plan() {
        // Test with empty keys list
        let mut old_ring = HashRing::default();
        let mut new_ring = HashRing::default();

        let node1 = PeerIdentifier("127.0.0.1:6379".into());
        let node2 = PeerIdentifier("127.0.0.1:6380".into());
        let repl_id1 = ReplicationId::Key(Uuid::now_v7().to_string());
        let repl_id2 = ReplicationId::Key(Uuid::now_v7().to_string());

        old_ring.add_partition_if_not_exists(repl_id1, node1);
        new_ring.add_partition_if_not_exists(repl_id2, node2);

        let empty_keys: Vec<String> = Vec::new();
        let tasks = old_ring.create_migration_tasks(&new_ring, empty_keys).await;

        // Should return empty migration tasks since no keys to migrate
        assert!(tasks.is_empty(), "Empty keys should result in no migration tasks");
    }
}
