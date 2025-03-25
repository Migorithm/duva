use crate::domains::{
    cluster_actors::commands::ConsensusClientResponse, peers::identifier::PeerIdentifier,
};
use tokio::sync::oneshot::Sender;
pub(crate) type ReplicationVote = Sender<ConsensusClientResponse>;

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    pub(crate) callback: T,
    pub(crate) cnt: u8,
    pub(crate) voters: Vec<PeerIdentifier>,
}
impl<T> ConsensusVoting<T> {
    pub(crate) fn increase_vote(&mut self, voter: PeerIdentifier) {
        self.cnt += 1;
        self.voters.push(voter);
    }

    fn get_required_votes(&self) -> u8 {
        ((self.voters.capacity() as f64 + 1.0) / 2.0).ceil() as u8
    }
    pub(crate) fn votable(&self, voter: &PeerIdentifier) -> bool {
        !self.voters.iter().any(|v| v == voter)
    }
}

impl ConsensusVoting<ReplicationVote> {
    pub(crate) fn send_result_or_pending(self, log_index: u64) -> Option<Self> {
        if self.cnt >= self.get_required_votes() {
            let _ = self.callback.send(ConsensusClientResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}
