use crate::domains::cluster_actors::hash_ring::HashRing;

use crate::domains::peers::peer::PeerState;

#[derive(bincode::Encode, bincode::Decode, Debug, PartialEq, Clone, Default)]
pub struct Topology {
    pub node_infos: Vec<PeerState>,
    pub hash_ring: HashRing,
}

impl Topology {
    pub fn new(node_info: Vec<PeerState>, hash_ring: HashRing) -> Self {
        Self { node_infos: node_info, hash_ring }
    }
}
