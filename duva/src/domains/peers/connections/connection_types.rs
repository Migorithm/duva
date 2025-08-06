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
        let replid = match (&self.replid, my_repl_id) {
            | (ReplicationId::Undecided, _) => my_repl_id.clone(),
            | (_, ReplicationId::Undecided) => self.replid.clone(),
            | _ => self.replid.clone(),
        };
        PeerState { id: self.id.clone(), match_index: self.hwm, replid, role: self.role.clone() }
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

impl PartialEq for ReadConnected {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ReadConnected {}
