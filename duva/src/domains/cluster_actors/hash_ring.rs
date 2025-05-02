use crate::domains::peers::peer::PeerState;
use crate::prelude::PeerIdentifier;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// A consistent hashing ring for distributing keys across nodes.
///
/// The `HashRing` maps keys to physical nodes using virtual nodes to ensure
/// even distribution. Each physical node is represented by multiple virtual
/// nodes on the ring, determined by `vnode_num`.
pub struct HashRing {
    vnodes: BTreeMap<u32, PeerIdentifier>,
    pnodes: HashMap<PeerIdentifier, PeerState>,
    vnode_num: usize,
}

impl HashRing {
    /// Creates a new `HashRing` with the specified number of virtual nodes per physical node.
    ///
    /// # Arguments
    /// * `virtual_nodes_per_physical` - Number of virtual nodes to create for each physical node.
    pub fn new(virtual_nodes_per_physical: usize) -> Self {
        Self {
            vnodes: BTreeMap::new(),
            pnodes: HashMap::new(),
            vnode_num: virtual_nodes_per_physical,
        }
    }

    /// Computes a 32-bit hash for a given value.
    ///
    /// # Arguments
    /// * `value` - The value to hash.
    ///
    /// # Returns
    /// A `u32` hash value.
    fn hash<T: Hash>(&self, value: &T) -> u32 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// Adds a physical node to the hash ring, creating its virtual nodes.
    ///
    /// # Arguments
    /// * `node` - The `PeerState` of the node to add.
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

    /// Removes a physical node and its virtual nodes from the hash ring.
    ///
    /// # Arguments
    /// * `node_id` - The identifier of the node to remove.
    pub fn remove_node(&mut self, pnode_id: &PeerIdentifier) {
        // Remove all virtual nodes for this physical node
        self.vnodes.retain(|_, peer_id| peer_id != pnode_id);

        // Remove the physical node
        self.pnodes.remove(pnode_id);
    }

    /// Retrieves the node responsible for a given key.
    ///
    /// # Arguments
    /// * `key` - The key to find the responsible node for.
    ///
    /// # Returns
    /// An `Option` containing a reference to the `PeerState` of the responsible node, or `None` if the ring is empty.
    pub fn get_node_for_key(&self, key: impl Into<String>) -> Option<&PeerIdentifier> {
        let hash = self.hash(&key.into());

        // Find the first virtual node that's greater than or equal to the key's hash
        self.vnodes
            .range(hash..)
            .next()
            .or_else(|| self.vnodes.first_key_value())
            .map(|(_, peer_id)| peer_id)
    }

    /// Retrieves all nodes responsible for keys in the range from `start_key` to `end_key`.
    /// This is NOT for lexicographical range queries.
    ///
    /// # Arguments
    /// * `start_key` - The starting key of the range.
    /// * `end_key` - The ending key of the range.
    ///
    /// # Returns
    /// A vector of references to the `PeerState` of nodes responsible for the key range.
    pub fn get_nodes_for_key_range(&self, start_key: &str, end_key: &str) -> Vec<&PeerState> {
        let start_hash = self.hash(&start_key.to_string());
        let end_hash = self.hash(&end_key.to_string());

        let mut node_ids = HashSet::new();

        // Include the node responsible for start_key
        if let Some(p1) = self.get_node_for_key(start_key) {
            node_ids.insert(p1.clone());
        }

        // Collect nodes with virtual nodes in the hash range
        if start_hash > end_hash {
            // Wrap-around case
            for (_, peer_id) in self.vnodes.range(start_hash..) {
                node_ids.insert(peer_id.clone());
            }
            for (_, peer_id) in self.vnodes.range(..=end_hash) {
                node_ids.insert(peer_id.clone());
            }
        } else {
            // Normal case
            for (_, peer_id) in self.vnodes.range(start_hash..=end_hash) {
                node_ids.insert(peer_id.clone());
            }
        }

        // Convert node IDs to PeerState references
        node_ids.iter().filter_map(|id| self.pnodes.get(id).map(|state| state)).collect()
    }

    /// Returns the distribution of virtual nodes across physical nodes.
    ///
    /// # Returns
    /// A `HashMap` mapping each "physical" node to the number of virtual nodes it owns.
    pub fn get_node_distribution(&self) -> HashMap<PeerIdentifier, usize> {
        let mut distribution = HashMap::new();
        for peer_id in self.vnodes.values() {
            *distribution.entry(peer_id.clone()).or_insert(0) += 1;
        }
        distribution
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
    fn test_node_distribution() {
        let mut ring = HashRing::new(3);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");
        let node3 = create_test_node("127.0.0.1:6381");

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);

        let distribution = ring.get_node_distribution();
        assert_eq!(distribution.len(), 3);
        assert!(distribution.values().all(|&count| count == 3));
    }

    #[test]
    fn test_key_range_distribution() {
        let mut ring = HashRing::new(3);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");
        let node3 = create_test_node("127.0.0.1:6381");

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);

        let nodes = ring.get_nodes_for_key_range("a", "z");
        assert!(!nodes.is_empty());
        assert!(nodes.len() <= 3);

        let nodes_wrap = ring.get_nodes_for_key_range("z", "a");
        assert!(!nodes_wrap.is_empty());
        assert!(nodes_wrap.len() <= 3);

        let nodes_same = ring.get_nodes_for_key_range("a", "a");
        assert!(!nodes_same.is_empty());
        assert!(nodes_same.len() <= 3);

        let unique_nodes: HashSet<_> = nodes.iter().map(|n| &n.addr).collect();
        assert_eq!(unique_nodes.len(), nodes.len());

        let added_nodes: HashSet<_> = vec![
            "127.0.0.1:6379".to_string(),
            "127.0.0.1:6380".to_string(),
            "127.0.0.1:6381".to_string(),
        ]
        .into_iter()
        .collect();

        for node in nodes {
            assert!(added_nodes.contains(&node.addr.to_string()));
        }
    }

    #[test]
    fn test_consistent_hashing() {
        let mut ring = HashRing::new(3);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");

        ring.add_node(node1);
        ring.add_node(node2);

        let key = "test_key";
        let node1 = ring.get_node_for_key(key);
        let node2 = ring.get_node_for_key(key);
        assert_eq!(node1, node2);
    }

    #[test]
    fn test_node_removal_redistribution() {
        let mut ring = HashRing::new(3);
        let node1 = create_test_node("127.0.0.1:6379");
        let node2 = create_test_node("127.0.0.1:6380");
        let node3 = create_test_node("127.0.0.1:6381");

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);

        let mut before_removal = Vec::new();
        for i in 0..100 {
            let key = format!("key{}", i);
            if let Some(node) = ring.get_node_for_key(&key) {
                before_removal.push((key, node.clone()));
            }
        }

        ring.remove_node(&"127.0.0.1:6379".to_string().into());

        let mut redistributed = 0;
        for (key, old_addr) in before_removal {
            if let Some(new_node) = ring.get_node_for_key(&key) {
                if *new_node != old_addr {
                    redistributed += 1;
                }
            }
        }

        assert!(redistributed > 0);
        assert!(redistributed < 100);
    }

    #[test]
    fn test_virtual_node_consistency() {
        let mut ring = HashRing::new(3);
        let node = create_test_node("127.0.0.1:6379");
        let node_id = node.addr.clone();
        ring.add_node(node);

        let virtual_nodes = ring.get_virtual_nodes();
        assert_eq!(virtual_nodes.len(), 3);

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
