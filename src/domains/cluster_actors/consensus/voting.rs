use crate::domains::{
    cluster_actors::commands::ConsensusClientResponse, peers::identifier::PeerIdentifier,
};
use tokio::sync::oneshot::Sender;
pub(crate) type ReplicationVote = Sender<ConsensusClientResponse>;

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    pub(crate) callback: T,
    pub(crate) pos_vt: u8,
    pub(crate) neg_vt: u8,
    pub(crate) replica_count: usize,
    pub(crate) voters: Vec<PeerIdentifier>,
}
impl<T> ConsensusVoting<T> {
    pub(crate) fn increase_vote(&mut self, voter: PeerIdentifier) {
        if self.voters.iter().any(|v| v == &voter) {
            return;
        }
        self.pos_vt += 1;
        self.voters.push(voter);
    }

    fn get_required_votes(&self) -> u8 {
        ((self.replica_count as f64 + 1.0) / 2.0).ceil() as u8
    }
}

impl ConsensusVoting<ReplicationVote> {
    pub(crate) fn maybe_not_finished(self, log_index: u64) -> Option<Self> {
        if self.pos_vt >= self.get_required_votes() {
            let _ = self.callback.send(ConsensusClientResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}
