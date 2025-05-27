use crate::{
    domains::{
        cluster_actors::replication::ReplicationId,
        peers::peer::{NodeKind, PeerState},
    },
    prelude::PeerIdentifier,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectedPeerInfo {
    pub(crate) id: PeerIdentifier,
    pub(crate) replid: ReplicationId,
    pub(crate) hwm: u64,
}

impl ConnectedPeerInfo {
    pub(crate) fn decide_peer_kind(&self, my_repl_id: &ReplicationId) -> PeerState {
        match (my_repl_id, &self.replid) {
            // Peer is undecided - assign as replica with our replication ID
            | (_, ReplicationId::Undecided) => {
                PeerState::new(&self.id, self.hwm, my_repl_id.clone(), NodeKind::Replica)
            },
            // I am undecided - adopt peer's replication ID
            | (ReplicationId::Undecided, _) => {
                PeerState::new(&self.id, self.hwm, self.replid.clone(), NodeKind::Replica)
            },
            // Matching replication IDs - regular replica
            | (my_id, peer_id) if my_id == peer_id => {
                PeerState::new(&self.id, self.hwm, self.replid.clone(), NodeKind::Replica)
            },
            // Different replication IDs - non-data peer
            | _ => PeerState::new(&self.id, self.hwm, self.replid.clone(), NodeKind::NonData),
        }
    }
}
