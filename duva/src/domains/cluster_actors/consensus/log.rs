use std::collections::HashMap;

use crate::{
    domains::{
        cluster_actors::{commands::ConsensusClientResponse, session::SessionRequest},
        peers::identifier::PeerIdentifier,
    },
    make_smart_pointer,
};
use tokio::sync::oneshot::Sender;
pub(crate) type ReplicationVote = Sender<ConsensusClientResponse>;

#[derive(Default, Debug)]
pub struct LogConsensusTracker(pub(crate) HashMap<u64, LogConsensusVoting>);
impl LogConsensusTracker {
    pub(crate) fn add(
        &mut self,
        key: u64,
        callback: Sender<ConsensusClientResponse>,
        replica_count: usize,
        session_req: Option<SessionRequest>,
    ) {
        self.insert(key, LogConsensusVoting::new(callback, replica_count, session_req));
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<u64, LogConsensusVoting>);

#[derive(Debug)]
pub struct LogConsensusVoting {
    pub(crate) voters: Vec<PeerIdentifier>,
    pub(crate) callback: ReplicationVote,
    pub(crate) cnt: u8,
    pub(crate) session_req: Option<SessionRequest>,
}
impl LogConsensusVoting {
    fn new(
        callback: ReplicationVote,
        replica_count: usize,
        session_req: Option<SessionRequest>,
    ) -> Self {
        Self { callback, cnt: 0, voters: Vec::with_capacity(replica_count), session_req }
    }

    pub(crate) fn increase_vote(&mut self, voter: PeerIdentifier) {
        self.cnt += 1;
        self.voters.push(voter);
    }

    pub(crate) fn get_required_votes(&self) -> u8 {
        let replica_count = self.voters.capacity() as u8;
        (replica_count + 1).div_ceil(2)
    }

    pub(crate) fn votable(&self, voter: &PeerIdentifier) -> bool {
        !self.voters.iter().any(|v| v == voter)
    }
}
