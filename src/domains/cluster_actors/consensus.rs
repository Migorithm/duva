use std::collections::HashMap;

use tokio::sync::oneshot::Sender;

use crate::{domains::append_only_files::log::LogIndex, make_smart_pointer};

use super::commands::WriteConsensusResponse;

#[derive(Debug)]
pub struct ConsensusVoting {
    sender: Sender<WriteConsensusResponse>,
    vote_count: u8,
    required_votes: u8,
}
impl ConsensusVoting {
    pub fn apply_vote(&mut self) {
        self.vote_count += 1;
    }

    pub fn maybe_not_finished(self, log_index: LogIndex) -> Option<Self> {
        if self.vote_count >= self.required_votes {
            let _ = self.sender.send(WriteConsensusResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}

#[derive(Default, Debug)]
pub struct ConsensusTracker(HashMap<LogIndex, ConsensusVoting>);
impl ConsensusTracker {
    pub fn add(
        &mut self,
        key: LogIndex,
        value: Sender<WriteConsensusResponse>,
        replica_count: usize,
    ) {
        self.0.insert(
            key,
            ConsensusVoting {
                sender: value,
                vote_count: 0,
                required_votes: ((replica_count as f64 + 1.0) / 2.0).ceil() as u8,
            },
        );
    }
    pub fn take(&mut self, offset: &LogIndex) -> Option<ConsensusVoting> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(ConsensusTracker, HashMap<LogIndex, ConsensusVoting>);
