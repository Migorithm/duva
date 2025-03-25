use crate::domains::{
    cluster_actors::commands::ConsensusClientResponse, peers::identifier::PeerIdentifier,
};
use tokio::sync::oneshot::Sender;
pub(crate) type ReplicationVote = Sender<ConsensusClientResponse>;

#[derive(Debug)]
pub struct ConsensusVoting {
    pub(crate) callback: ReplicationVote,
    pub(crate) cnt: u8,
    pub(crate) voters: Vec<PeerIdentifier>,
}
impl ConsensusVoting {
    fn increase_vote(&mut self, voter: PeerIdentifier) {
        self.cnt += 1;
        self.voters.push(voter);
    }

    fn get_required_votes(&self) -> u8 {
        ((self.voters.capacity() as f64 + 1.0) / 2.0).ceil() as u8
    }
    fn votable(&self, voter: &PeerIdentifier) -> bool {
        !self.voters.iter().any(|v| v == voter)
    }
}

impl ConsensusVoting {
    pub(crate) fn vote_and_maybe_stay_pending(
        mut self,
        log_idx: u64,
        from: PeerIdentifier,
    ) -> Option<Self> {
        if self.votable(&from) {
            println!("[INFO] Received acks for log index num: {}", log_idx);
            self.increase_vote(from);
        } else {
            return Some(self);
        }

        if self.cnt >= self.get_required_votes() {
            let _ = self.callback.send(ConsensusClientResponse::LogIndex(Some(log_idx)));
            None
        } else {
            Some(self)
        }
    }
}
