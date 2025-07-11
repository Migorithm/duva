use crate::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use crate::domains::peers::peer::PeerState;
use crate::domains::{TRead, TWrite};
use crate::prelude::PeerIdentifier;

use crate::make_smart_pointer;

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectedPeerInfo {
    pub(crate) id: PeerIdentifier,
    pub(crate) replid: ReplicationId,
    pub(crate) hwm: u64,
    pub(crate) role: ReplicationRole,
}

impl ConnectedPeerInfo {
    pub(crate) fn decide_peer_state(&self, my_repl_id: &ReplicationId) -> PeerState {
        match (my_repl_id, &self.replid) {
            // Peer is undecided - assign as replica with our replication ID
            | (_, ReplicationId::Undecided) => {
                PeerState::new(&self.id, self.hwm, my_repl_id.clone(), self.role.clone())
            },
            // I am undecided - adopt peer's replication ID
            | (ReplicationId::Undecided, _) => {
                PeerState::new(&self.id, self.hwm, self.replid.clone(), self.role.clone())
            },
            // Matching replication IDs - regular replica
            | (my_id, peer_id) if my_id == peer_id => {
                PeerState::new(&self.id, self.hwm, self.replid.clone(), self.role.clone())
            },
            // Different replication IDs - non-data peer
            | _ => PeerState::new(&self.id, self.hwm, self.replid.clone(), self.role.clone()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct WriteConnected(pub(crate) Box<dyn TWrite>);
make_smart_pointer!(WriteConnected, Box<dyn TWrite>);

impl<T: TWrite> From<T> for WriteConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

impl PartialEq for WriteConnected {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for WriteConnected {}

#[derive(Debug)]
pub(crate) struct ReadConnected(pub(crate) Box<dyn TRead>);
make_smart_pointer!(ReadConnected, Box<dyn TRead>);

impl<T: TRead> From<T> for ReadConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}
