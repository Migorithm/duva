use crate::domains::cluster_actors::hash_ring::HashRing;
use crate::domains::cluster_actors::replication::{
    ReplicationId, ReplicationInfo, ReplicationRole,
};
use crate::domains::peers::peer::PeerState;
use crate::prelude::PeerIdentifier;

#[derive(bincode::Encode, bincode::Decode, Debug, PartialEq, Clone, Default)]
pub struct Topology {
    pub node_infos: Vec<NodeReplInfo>,
    pub hash_ring: HashRing,
}

impl Topology {
    pub fn new(node_info: Vec<NodeReplInfo>, hash_ring: HashRing) -> Self {
        Self { node_infos: node_info, hash_ring }
    }
}

#[derive(bincode::Encode, bincode::Decode, Debug, PartialEq, Clone)]
pub struct NodeReplInfo {
    pub peer_id: PeerIdentifier,
    pub repl_id: ReplicationId,
    pub repl_role: ReplicationRole,
}

impl NodeReplInfo {
    pub fn from_peer_state(peer_state: &PeerState) -> Self {
        Self {
            peer_id: peer_state.id().clone(),
            repl_id: peer_state.replid.clone(),
            repl_role: peer_state.role.clone(),
        }
    }

    pub(crate) fn from_replication_state(replication_state: ReplicationInfo) -> Self {
        Self {
            peer_id: PeerIdentifier::new(&replication_state.self_host, replication_state.self_port),
            repl_id: replication_state.replid.clone(),
            repl_role: replication_state.role.clone(),
        }
    }
}
