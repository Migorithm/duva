use std::fmt::Display;

use super::identifier::PeerIdentifier;

#[derive(Clone, Debug)]
pub enum PeerKind {
    Follower { hwm: u64, leader_repl_id: PeerIdentifier },
    Leader,

    PFollower { leader_repl_id: PeerIdentifier },
    PLeader,
}

impl PeerKind {
    pub fn accepted_peer_kind(my_repl_id: &str, peer_repl_id: &str, peer_hwm: u64) -> Self {
        if my_repl_id == peer_repl_id {
            return Self::Follower { hwm: peer_hwm, leader_repl_id: peer_repl_id.into() };
        }

        Self::Leader
    }
    pub fn connected_peer_kind(
        my_repl_id: &str,
        peer_id: &str,
        peer_repl_id: &str,
        peer_hwm: u64,
    ) -> Self {
        if my_repl_id == peer_id {
            return Self::Leader;
        }
        if my_repl_id == peer_repl_id {
            return Self::Follower { hwm: peer_hwm, leader_repl_id: peer_repl_id.into() };
        }

        if peer_id == peer_repl_id {
            return Self::PLeader;
        }
        Self::PFollower { leader_repl_id: peer_repl_id.into() }
    }
}

impl Display for PeerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerKind::Follower { hwm, leader_repl_id } => write!(f, "follower {}", leader_repl_id),
            PeerKind::Leader => write!(f, "leader - 0"),
            PeerKind::PFollower { leader_repl_id } => write!(f, "follower {}", leader_repl_id),
            PeerKind::PLeader => write!(f, "leader - 0"),
        }
    }
}
