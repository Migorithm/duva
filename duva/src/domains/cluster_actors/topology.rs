use crate::domains::cluster_actors::hash_ring::HashRing;

use crate::domains::peers::peer::NodeState;

#[derive(bincode::Encode, bincode::Decode, Debug, PartialEq, Clone, Default)]
pub struct Topology {
    pub node_states: Vec<NodeState>,
    pub hash_ring: HashRing,
}
