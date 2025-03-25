pub mod voting;
use super::commands::ConsensusClientResponse;
use crate::{domains::peers::identifier::PeerIdentifier, make_smart_pointer};

use std::collections::HashMap;
use tokio::sync::oneshot::Sender;
use voting::ConsensusVoting;

#[derive(Default, Debug)]
pub struct LogConsensusTracker(
    pub(crate) HashMap<u64, ConsensusVoting<Sender<ConsensusClientResponse>>>,
);
impl LogConsensusTracker {
    pub(crate) fn add(
        &mut self,
        key: u64,
        value: Sender<ConsensusClientResponse>,
        replica_count: usize,
    ) {
        self.insert(
            key,
            ConsensusVoting {
                callback: value,
                cnt: 0, // no need for self vote
                voters: Vec::with_capacity(replica_count),
            },
        );
    }

    pub(crate) fn track_progress(&mut self, log_idx: u64, from: PeerIdentifier) {
        if let Some(consensus) = self.remove(&log_idx) {
            if let Some(consensus) = consensus.vote_and_maybe_stay_pending(log_idx, from) {
                self.insert(log_idx, consensus);
            }
        }
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<u64, ConsensusVoting<Sender<ConsensusClientResponse>>>);
