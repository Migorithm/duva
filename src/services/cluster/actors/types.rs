use crate::make_smart_pointer;

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
    pub fn connected_peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        Self::Master
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);
