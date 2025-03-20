use crate::domains::cluster_actors::commands::WriteConsensusResponse;
use tokio::sync::oneshot::Sender;

pub(crate) type ReplicationVote = Sender<WriteConsensusResponse>;

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    pub(crate) callback: T,
    pub(crate) pos_vt: u8,
    pub(crate) neg_vt: u8,
    pub(crate) replica_count: usize,
}
impl<T> ConsensusVoting<T> {
    pub(crate) fn increase_vote(&mut self) {
        self.pos_vt += 1;
    }

    fn get_required_votes(&self) -> u8 {
        ((self.replica_count as f64 + 1.0) / 2.0).ceil() as u8
    }
}

impl ConsensusVoting<ReplicationVote> {
    pub(crate) fn maybe_not_finished(self, log_index: u64) -> Option<Self> {
        if self.pos_vt >= self.get_required_votes() {
            let _ = self.callback.send(WriteConsensusResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}
