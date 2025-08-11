use crate::prelude::PeerIdentifier;
/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.
use crate::{ReplicationId, make_smart_pointer};
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
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
    pnodes: HashMap<ReplicationId, PeerIdentifier>,
    pub(crate) last_modified: u128,
}

impl HashRing {
    fn update_last_modified(&mut self) {
        self.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
    }

    /// Sets the hash ring partitions to exactly match the provided list.
    /// This method will not respect any existing partitions that are not in the given partitions
    ///
    /// Returns None if the new partitions are identical to the current ones.
    pub(crate) fn set_partitions(
        &self,
        partitions: Vec<(ReplicationId, PeerIdentifier)>,
    ) -> Option<HashRing> {
        // Create a set of new replication IDs for easy comparison
        let new_repl_ids: std::collections::HashSet<_> =
            partitions.iter().map(|(repl_id, _)| repl_id).collect();

        // Check if any changes are needed
        let current_repl_ids: std::collections::HashSet<_> = self.pnodes.keys().collect();

        // If the sets are identical and all peer identifiers match, no changes needed
        if new_repl_ids == current_repl_ids {
            let all_unchanged = partitions
                .iter()
                .all(|(repl_id, peer_id)| self.pnodes.get(repl_id) == Some(peer_id));
            if all_unchanged {
                return None;
            }
        }
        // Create a new hash ring with only the specified partitions
        let mut ring = HashRing::default().add_partitions(partitions);
        ring.update_last_modified();
        Some(ring)
    }

    pub(crate) fn add_partitions(
        mut self,
        partitions: Vec<(ReplicationId, PeerIdentifier)>,
    ) -> HashRing {
        // Add all specified partitions
        for (repl_id, leader_id) in partitions {
            self.pnodes.insert(repl_id.clone(), leader_id);

            let repl_id = Rc::new(repl_id);
            // Create virtual nodes for better distribution
            for i in 0..V_NODE_NUM {
                let virtual_node_id = format!("{repl_id}-{i}");
                let hash = fnv_1a_hash(&virtual_node_id);
                self.vnodes.insert(hash, repl_id.clone());
            }
        }
        self
    }

    fn find_replid(&self, hash: u64) -> Option<&ReplicationId> {
        // Find the first vnode with hash >= target hash
        self.vnodes
            .range(hash..)
            .next()
            .or_else(|| self.vnodes.iter().next()) // wrap around to first node
            .map(|(_, node_id)| node_id.as_ref())
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
            if let Some(old_owner) = self.find_replid(token)
                && let Some(new_owner) = new_ring.find_replid(token)
            {
                if old_owner == new_owner {
                    continue;
                }

                // Node ownership changed for this partition
                // Need to migrate data from old node to new node
                let prev_token = if i == 0 { tokens[tokens.len() - 1] } else { tokens[i - 1] };
                let (start, end) = (prev_token.wrapping_add(1), token);
                let keys_to_migrate = filter_keys_in_partition(&keys, start, end);
                if !keys_to_migrate.is_empty() {
                    migration_tasks
                        .entry(new_owner.clone())
                        .or_default()
                        .push(MigrationTask { range: (start, end), keys_to_migrate });
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

    pub fn key_ownership<'a>(
        &self,
        keys: impl Iterator<Item = &'a str>,
    ) -> anyhow::Result<KeyOwnership<'a>> {
        keys.into_iter()
            .try_fold(
                HashMap::<ReplicationId, Vec<&str>>::new(),
                |mut map, key| -> Result<_, anyhow::Error> {
                    let hash = fnv_1a_hash(key);
                    let replid = self
                        .find_replid(hash)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("No node found for key: {}", key))?;
                    map.entry(replid).or_default().push(key);
                    Ok(map)
                },
            )
            .map(KeyOwnership)
    }

    #[cfg(test)]
    pub fn get_node_for_key(&self, key: &str) -> Option<&ReplicationId> {
        let hash = fnv_1a_hash(key);
        self.find_replid(hash)
    }

    pub(crate) fn update_repl_leader(&mut self, replid: ReplicationId, new_pnode: PeerIdentifier) {
        if let Some(existing_pnode) = self.pnodes.get_mut(&replid)
            && existing_pnode != &new_pnode
        {
            *existing_pnode = new_pnode;
        }
        self.update_last_modified();
    }

    pub fn get_replication_ids(&self) -> Vec<ReplicationId> {
        self.pnodes.keys().cloned().collect()
    }
}

pub struct KeyOwnership<'a>(HashMap<ReplicationId, Vec<&'a str>>);
impl<'a> KeyOwnership<'a> {
    pub(crate) fn all_belongs_to(&self, target: &ReplicationId) -> bool {
        self.keys().all(|replid| replid == target)
    }
    pub(crate) fn except(self, target: &ReplicationId) -> Vec<&'a str> {
        self.iter()
            .filter_map(|(replid, keys)| if replid != target { Some(keys.iter()) } else { None })
            .flatten()
            .copied()
            .collect::<Vec<_>>()
    }
}
impl<'a> Deref for KeyOwnership<'a> {
    type Target = HashMap<ReplicationId, Vec<&'a str>>;
    fn deref(&self) -> &Self::Target {
        &self.0
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
