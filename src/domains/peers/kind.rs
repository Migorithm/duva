use std::fmt::Display;

use super::{connected_peer_info::ConnectedPeerInfo, identifier::PeerIdentifier};

#[derive(Clone, Debug)]
pub enum PeerKind {
    Follower { hwm: u64, leader_repl_id: PeerIdentifier },
    Leader,
    PFollower { leader_repl_id: PeerIdentifier },
    PLeader,
}

impl PeerKind {
    pub fn decide_peer_kind(my_repl_id: &str, peer_info: ConnectedPeerInfo) -> Self {
        if my_repl_id == *peer_info.id {
            return Self::Leader;
        }
        if my_repl_id == *peer_info.leader_repl_id {
            return Self::Follower { hwm: peer_info.hwm, leader_repl_id: peer_info.leader_repl_id };
        }
        if peer_info.id == peer_info.leader_repl_id {
            return Self::PLeader;
        }
        Self::PFollower { leader_repl_id: peer_info.leader_repl_id }
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
