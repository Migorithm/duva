use crate::domains::peers::peer::PeerState;
use crate::prelude::PeerIdentifier;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::ops::Range;

pub struct HashRing {
    virtual_nodes: BTreeMap<u32, NodeInfo>,
    physical_nodes: HashMap<PeerIdentifier, PhysicalNode>,
    vnode_num: usize,
}

struct NodeInfo {
    physical_node_id: PeerIdentifier,
    node: PeerState,
}

struct PhysicalNode {
    hash_range: Range<u32>,
    state: PeerState,
}

impl HashRing {
    pub fn new(vnode_num: usize) -> Self {
        Self { virtual_nodes: BTreeMap::new(), physical_nodes: HashMap::new(), vnode_num }
    }

    fn hash<T: Hash>(&self, value: &T) -> u32 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish() as u32
    }

    pub fn add_node(&mut self, node: PeerState) {
        let node_id = node.addr.clone();

        // Create virtual nodes for better distribution
        for i in 0..self.vnode_num {
            let virtual_node_id = format!("{}-{}", node_id, i);
            let hash = self.hash(&virtual_node_id);

            self.virtual_nodes
                .insert(hash, NodeInfo { physical_node_id: node_id.clone(), node: node.clone() });
        }

        // Calculate the hash range for this physical node
        let hash_range = self.calculate_node_range(&node_id);

        // Update physical node mapping
        self.physical_nodes.insert(node_id, PhysicalNode { hash_range, state: node });
    }

    pub fn remove_node(&mut self, node_id: &PeerIdentifier) {
        // Remove all virtual nodes for this physical node
        self.virtual_nodes.retain(|_, node_info| &node_info.physical_node_id != node_id);

        // Remove the physical node
        self.physical_nodes.remove(node_id);

        // Recalculate hash ranges for remaining nodes
        self.recalculate_ranges();
    }

    pub fn get_node_for_key(&self, key: impl Into<String>) -> Option<&PeerState> {
        let hash = self.hash(&key.into());

        // Find the first virtual node that's greater than or equal to the key's hash
        self.virtual_nodes
            .range(hash..)
            .next()
            .or_else(|| self.virtual_nodes.first_key_value())
            .map(|(_, node_info)| &node_info.node)
    }

    fn calculate_node_range(&self, node_id: &PeerIdentifier) -> Range<u32> {
        // Get all virtual nodes for this physical node
        let mut hashes: Vec<u32> = self
            .virtual_nodes
            .iter()
            .filter(|(_, node_info)| &node_info.physical_node_id == node_id)
            .map(|(hash, _)| *hash)
            .collect();

        hashes.sort();

        if hashes.is_empty() {
            return 0..0;
        }

        hashes[0]..hashes[hashes.len() - 1] + 1
    }

    fn recalculate_ranges(&mut self) {
        // Recalculate hash ranges for all physical nodes
        for node_id in self.physical_nodes.keys().cloned().collect::<Vec<_>>() {
            let hash_range = self.calculate_node_range(&node_id);
            if let Some(physical_node) = self.physical_nodes.get_mut(&node_id) {
                physical_node.hash_range = hash_range;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::cluster_actors::replication::ReplicationId;
    use crate::domains::peers::peer::NodeKind;

    fn create_test_node(addr: &str) -> PeerState {
        PeerState::new(addr, 0, ReplicationId::Key("test".to_string()), NodeKind::Replica)
    }

    #[test]
    fn test_add_and_remove_node() {
        let mut ring = HashRing::new(3);
        let node = create_test_node("127.0.0.1:6379");
        let node_id = node.addr.clone();

        // Add node
        ring.add_node(node);
        assert_eq!(ring.physical_nodes.len(), 1);
        assert_eq!(ring.virtual_nodes.len(), 3);

        // Remove node
        ring.remove_node(&node_id);
        assert_eq!(ring.physical_nodes.len(), 0);
        assert_eq!(ring.virtual_nodes.len(), 0);
    }

    #[test]
    fn test_get_node_for_key() {
        let mut ring = HashRing::new(3);
        let node = create_test_node("127.0.0.1:6379");
        ring.add_node(node);

        // Test key distribution
        let key = "test_key";
        let node = ring.get_node_for_key(key);
        assert!(node.is_some());
    }

    #[test]
    fn test_multiple_nodes() {
        let mut ring = HashRing::new(3);

        // Add multiple nodes
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");

        ring.add_node(node1);
        ring.add_node(node2);

        assert_eq!(ring.physical_nodes.len(), 2);
        assert_eq!(ring.virtual_nodes.len(), 6);
    }
}
