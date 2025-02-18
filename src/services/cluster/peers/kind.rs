#[derive(Clone, Debug)]
pub enum PeerKind {
    Peer,
    Follower,
    Leader,
}

impl PeerKind {
    pub fn accepted_peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        match other_repl_id {
            "?" => Self::Follower,
            id if id == self_repl_id => Self::Follower,
            _ => Self::Peer,
        }
    }
    pub fn connected_peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        if self_repl_id == "?" {
            Self::Leader
        } else if self_repl_id == other_repl_id {
            Self::Follower
        } else {
            Self::Peer
        }
    }
}
