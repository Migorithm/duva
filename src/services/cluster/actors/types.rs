use crate::make_smart_pointer;

#[derive(Clone, PartialEq, Debug)]
pub enum PeerKind {
    Peer,
    Replica,
    Master,
}

impl PeerKind {
    pub fn peer_kind(self_repl_id: &str, incoming_stream_repl_id: &str) -> Self {
        if incoming_stream_repl_id == "?" || self_repl_id == incoming_stream_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);
