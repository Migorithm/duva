use crate::make_smart_pointer;

use super::replication::Replication;

#[derive(Clone, Debug)]
pub enum PeerKind {
    Peer,
    Replica,
    Master,
}

impl PeerKind {
    pub fn accepted_peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        if other_repl_id == "?" || self_repl_id == other_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
    pub fn connected_peer_kind(self_repl_info: &Replication, other_repl_id: &str) -> Self {
        if self_repl_info.master_replid == "?" {
            Self::Master
        } else if self_repl_info.master_replid == other_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);
