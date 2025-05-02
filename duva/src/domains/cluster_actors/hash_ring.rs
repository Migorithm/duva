use crate::domains::peers::peer::PeerState;
use crate::prelude::PeerIdentifier;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.
///
#[derive(Debug)]
pub struct HashRing {
    vnodes: BTreeMap<u32, PeerIdentifier>,
    pnodes: HashMap<PeerIdentifier, PeerState>,
    vnode_num: usize, // Number of virtual nodes to create for each physical node.
}

impl HashRing {
    pub fn new(vnode_num: usize) -> Self {
        Self { vnodes: BTreeMap::new(), pnodes: HashMap::new(), vnode_num }
    }

    fn hash<T: Hash>(&self, value: &T) -> u32 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish() as u32
    }

    pub fn add_node(&mut self, peer_state: PeerState) {
        let pnode_id = peer_state.addr.clone();

        // Create virtual nodes for better distribution
        for i in 0..self.vnode_num {
            let virtual_node_id = format!("{}-{}", pnode_id, i);
            let hash = self.hash(&virtual_node_id);

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

    pub fn get_node_for_key(&self, key: impl Into<String>) -> Option<&PeerIdentifier> {
        let hash = self.hash(&key.into());

        // * Find the first virtual node that's greater than or equal to the key's hash
        self.vnodes
            .range(hash..)
            .next()
            .or_else(|| self.vnodes.first_key_value())
            .map(|(_, peer_id)| peer_id)
    }

    #[cfg(test)]
    fn get_virtual_nodes(&self) -> Vec<(&u32, &PeerIdentifier)> {
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
