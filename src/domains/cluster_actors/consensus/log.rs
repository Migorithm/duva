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
    pub(crate) fn track_progress(&mut self, log_idx: u64, from: PeerIdentifier) {
        if let Some(consensus) = self.remove(&log_idx) {
            if let Some(consensus) = consensus.vote_and_maybe_stay_pending(log_idx, from) {
                self.insert(log_idx, consensus);
            }
        }
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<u64, LogConsensusVoting>);

#[derive(Debug)]
pub struct LogConsensusVoting {
    pub(crate) voters: Vec<PeerIdentifier>,
    callback: ReplicationVote,
    cnt: u8,
    session_req: Option<SessionRequest>,
}
impl LogConsensusVoting {
    fn new(
        callback: ReplicationVote,
        replica_count: usize,
        session_req: Option<SessionRequest>,
    ) -> Self {
        Self { callback, cnt: 0, voters: Vec::with_capacity(replica_count), session_req }
    }

    fn vote_and_maybe_stay_pending(mut self, log_idx: u64, from: PeerIdentifier) -> Option<Self> {
        if self.votable(&from) {
            println!("[INFO] Received acks for log index num: {}", log_idx);
            self.increase_vote(from);
        }

        if self.cnt < self.get_required_votes() {
            return Some(self);
        }

        let _ = self.callback.send(ConsensusClientResponse::LogIndex(Some(log_idx)));
        None
    }

    fn increase_vote(&mut self, voter: PeerIdentifier) {
        self.cnt += 1;
        self.voters.push(voter);
    }

    fn get_required_votes(&self) -> u8 {
        let replica_count = self.voters.capacity() as u8;
        (replica_count + 1).div_ceil(2)
    }

    fn votable(&self, voter: &PeerIdentifier) -> bool {
        !self.voters.iter().any(|v| v == voter)
    }
}
