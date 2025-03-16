use tokio::sync::oneshot::Sender;

use crate::domains::{
    append_only_files::log::LogIndex, cluster_actors::commands::WriteConsensusResponse,
};

pub(crate) type ReplicationVote = Sender<WriteConsensusResponse>;
pub(crate) type ElectionVote = ();

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    pub(crate) callback: T,
    pub(crate) pos_vt: u8,
    pub(crate) neg_vt: u8,
    pub(crate) replica_count: usize,
}
impl<T> ConsensusVoting<T> {
    pub fn increase_vote(&mut self) {
        self.pos_vt += 1;
    }

    fn get_required_votes(&self) -> u8 {
        ((self.replica_count as f64 + 1.0) / 2.0).ceil() as u8
    }
}

impl ConsensusVoting<ReplicationVote> {
    pub fn maybe_not_finished(self, log_index: LogIndex) -> Option<Self> {
        if self.pos_vt >= self.get_required_votes() {
            let _ = self.callback.send(WriteConsensusResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}

impl ConsensusVoting<ElectionVote> {
    pub fn maybe_not_finished(mut self, granted: bool) -> Result<bool, Self> {
        if granted {
            self.increase_vote();
        } else {
            self.neg_vt += 1;
        }

        let required_count = self.get_required_votes();
        if self.pos_vt >= required_count {
            Ok(true)
        } else if self.neg_vt >= required_count {
            Ok(false)
        } else {
            Err(self)
        }
    }
}
