use std::fmt::Display;

use super::identifier::PeerIdentifier;

#[derive(Clone, Debug)]
pub enum PeerKind {
    Peer,
    Follower { hwm: u64, leader_repl_id: PeerIdentifier },
    Leader,
}

impl PeerKind {
    pub fn accepted_peer_kind(
        self_repl_id: &str,
        other_repl_id: &str,
        inbound_peer_hwm: u64,
    ) -> Self {
        match other_repl_id {
            "?" => Self::Follower { hwm: inbound_peer_hwm, leader_repl_id: other_repl_id.into() },
            id if id == self_repl_id => {
                Self::Follower { hwm: inbound_peer_hwm, leader_repl_id: other_repl_id.into() }
            },
            _ => Self::Peer,
        }
    }
    pub fn connected_peer_kind(
        self_repl_id: &str,
        other_repl_id: &str,
        inbound_peer_hwm: u64,
    ) -> Self {
        if self_repl_id == "?" {
            Self::Leader
        } else if self_repl_id == other_repl_id {
            Self::Follower { hwm: inbound_peer_hwm, leader_repl_id: other_repl_id.into() }
        } else {
            Self::Peer
        }
    }
}

impl Display for PeerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerKind::Peer => write!(f, "peer"),
            PeerKind::Follower { hwm, leader_repl_id } => write!(f, "follower {}", leader_repl_id),
            PeerKind::Leader => write!(f, "leader -"),
        }
    }
}
