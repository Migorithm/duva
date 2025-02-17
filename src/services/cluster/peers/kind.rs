use crate::services::cluster::actors::replication::ReplicationInfo;

#[derive(Clone, Debug)]
pub enum PeerKind {
    Peer,
    Replica,
    Master,
}

impl PeerKind {
    pub fn accepted_peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        match other_repl_id {
            "?" => Self::Replica,
            id if id == self_repl_id => Self::Replica,
            _ => Self::Peer,
        }
    }
    pub fn connected_peer_kind(self_repl_info: &ReplicationInfo, other_repl_id: &str) -> Self {
        if self_repl_info.master_replid == "?" {
            Self::Master
        } else if self_repl_info.master_replid == other_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
}
