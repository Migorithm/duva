/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.
use crate::ReplicationId;
use crate::prelude::PeerIdentifier;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
mod hash_func;
mod migration_task;
pub(crate) use hash_func::fnv_1a_hash;
pub(crate) use migration_task::*;

#[cfg(test)]
pub(crate) mod tests;

// Number of virtual nodes to create for each physical node.
const V_NODE_NUM: u16 = 256;

#[derive(Debug, Default, bincode::Decode, bincode::Encode, Clone, Eq)]
pub struct HashRing {
    vnodes: BTreeMap<u64, Rc<ReplicationId>>,
    // TODO value in the following map must be replaced when election happens
    pnodes: HashMap<ReplicationId, PeerIdentifier>,
    pub(crate) last_modified: u128,
}

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
    pub(crate) fn add_partition_if_not_exists(
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
    fn remove_partition(&mut self, target_repl_id: &ReplicationId) {
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

    pub(crate) fn verify_key_belongs_to_node(&self, keys: &[&str], node: &ReplicationId) -> bool {
        keys.iter().all(|key| {
            let key_hash = fnv_1a_hash(key);
            self.find_node(key_hash) == Some(node)
        })
    }

    pub(crate) fn create_migration_tasks(
        &self,
        new_ring: &HashRing,
        keys: Vec<String>,
    ) -> BTreeMap<ReplicationId, Vec<MigrationTask>> {
        let mut migration_tasks: BTreeMap<ReplicationId, Vec<MigrationTask>> = BTreeMap::new();

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
                        migration_tasks.entry(new_owner.clone()).or_default().push(MigrationTask {
                            task_id: (start, end),
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

// ! SAFETY: HashRing is supposed to be used in a single-threaded context
// ! with cluster actor as the actor is the access point to the ring.
unsafe impl Send for HashRing {}
unsafe impl Sync for HashRing {}

impl PartialEq for HashRing {
    fn eq(&self, other: &Self) -> bool {
        self.vnodes == other.vnodes && self.pnodes == other.pnodes
    }
}
