use crate::domains::cluster_actors::hash_ring::HashRing;
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::peers::peer::PeerState;

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
    pub peer_id: String,
    pub repl_id: String,
    pub repl_role: String,
}

impl NodeReplInfo {
    pub fn from_peer_state(peer_state: &PeerState) -> Self {
        Self {
            peer_id: peer_state.id().to_string(),
            repl_id: peer_state.replid.to_string(),
            repl_role: peer_state.role.to_string(),
        }
    }

    pub fn from_replication_state(replication_state: &ReplicationState) -> Self {
        Self {
            peer_id: replication_state.self_identifier().to_string(),
            repl_id: replication_state.replid.to_string(),
            repl_role: replication_state.role.to_string(),
        }
    }
}
