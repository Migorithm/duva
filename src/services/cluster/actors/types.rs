use crate::make_smart_pointer;

#[derive(Clone)]
pub enum PeerKind {
    Peer,
    Replica,
    Master,
}

impl PeerKind {
    pub fn peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        if self_repl_id == "?" || self_repl_id == other_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);
