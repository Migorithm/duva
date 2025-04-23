use crate::domains::cluster_actors::replication::ReplicationId;

use super::{identifier::PeerIdentifier, peer::PeerState};

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectedPeerInfo {
    // TODO repl_id here is the leader_replid from connected server.
    pub(crate) id: PeerIdentifier,
    pub(crate) replid: ReplicationId,
    pub(crate) hwm: u64,
    pub(crate) peer_list: Vec<String>,
}

impl ConnectedPeerInfo {
    pub(crate) fn list_peer_binding_addrs(&mut self) -> Vec<PeerIdentifier> {
        std::mem::take(&mut self.peer_list).into_iter().map(Into::into).collect::<Vec<_>>()
    }

    pub(crate) fn decide_peer_kind(&self, my_repl_id: &ReplicationId) -> PeerState {
        match (my_repl_id, &self.replid) {
            // Peer is undecided - assign as replica with our replication ID
            (_, ReplicationId::Undecided) => {
                PeerState::Replica { match_index: self.hwm, replid: my_repl_id.clone() }
            },
            // I am undecided - adopt peer's replication ID
            (ReplicationId::Undecided, _) => {
                PeerState::Replica { match_index: self.hwm, replid: self.replid.clone() }
            },
            // Matching replication IDs - regular replica
            (my_id, peer_id) if my_id == peer_id => {
                PeerState::Replica { match_index: self.hwm, replid: self.replid.clone() }
            },
            // Different replication IDs - non-data peer
            _ => PeerState::NonDataPeer { match_index: self.hwm, replid: self.replid.clone() },
        }
    }
}
