use crate::domains::cluster_actors::hash_ring::HashRing;
use crate::prelude::PeerIdentifier;

#[derive(bincode::Encode, bincode::Decode, Debug, PartialEq, Clone, Default)]
pub struct Topology {
    pub connected_peers: Vec<PeerIdentifier>,
    pub hash_ring: HashRing,
}

impl Topology {
    pub fn new(connected_peers: Vec<PeerIdentifier>, hash_ring: HashRing) -> Self {
        Self { connected_peers, hash_ring }
    }
}
