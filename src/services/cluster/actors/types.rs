use crate::{from_to, make_smart_pointer};

use super::replication::Replication;

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
pub struct PeerIdentifier(pub String);
make_smart_pointer!(PeerIdentifier, String);
from_to!(String, PeerIdentifier);

pub struct PeerAddrs(pub Vec<PeerIdentifier>);
make_smart_pointer!(PeerAddrs, Vec<PeerIdentifier>);
from_to!(Vec<PeerIdentifier>, PeerAddrs);
impl PeerAddrs {
    pub fn stringify(self) -> String {
        self.0.into_iter().map(|x| x.0).collect::<Vec<String>>().join(" ")
    }
}
