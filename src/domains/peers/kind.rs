#[derive(Clone, Debug)]
pub enum PeerKind {
    Peer,
    Follower(u64),
    Leader,
}

impl PeerKind {
    pub fn accepted_peer_kind(
        self_repl_id: &str,
        other_repl_id: &str,
        inbound_peer_offset: u64,
    ) -> Self {
        match other_repl_id {
            "?" => Self::Follower(inbound_peer_offset),
            id if id == self_repl_id => Self::Follower(inbound_peer_offset),
            _ => Self::Peer,
        }
    }
    pub fn connected_peer_kind(
        self_repl_id: &str,
        other_repl_id: &str,
        inbound_peer_offset: u64,
    ) -> Self {
        if self_repl_id == "?" {
            Self::Leader
        } else if self_repl_id == other_repl_id {
            Self::Follower(inbound_peer_offset)
        } else {
            Self::Peer
        }
    }
}
