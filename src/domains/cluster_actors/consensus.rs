use std::collections::HashMap;

use tokio::sync::oneshot::Sender;

use crate::make_smart_pointer;

pub struct ConsensusVoting {
    sender: Sender<Option<u64>>,
    vote_count: u8,
    required_votes: u8,
}
impl ConsensusVoting {
    pub fn apply_vote(&mut self) {
        self.vote_count += 1;
    }

    pub fn maybe_not_finished(self, offset: u64) -> Option<Self> {
        if self.vote_count >= self.required_votes {
            let _ = self.sender.send(Some(offset));
            None
        } else {
            Some(self)
        }
    }
}

#[derive(Default)]
pub struct ConsensusTracker(HashMap<u64, ConsensusVoting>);
impl ConsensusTracker {
    pub fn add(&mut self, key: u64, value: Sender<Option<u64>>, replica_count: usize) {
        self.0.insert(
            key,
            ConsensusVoting {
                sender: value,
                vote_count: 0,
                required_votes: ((replica_count as f64 + 1.0) / 2.0).ceil() as u8,
            },
        );
    }
    pub fn take(&mut self, offset: &u64) -> Option<ConsensusVoting> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(ConsensusTracker, HashMap<u64, ConsensusVoting>);
