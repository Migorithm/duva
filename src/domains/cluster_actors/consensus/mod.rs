pub mod voting;
use super::commands::ConsensusClientResponse;
use crate::make_smart_pointer;

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
        self.0.insert(
            key,
            ConsensusVoting {
                callback: value,
                pos_vt: 0, // no need for self vote
                neg_vt: 0,
                replica_count,
                voters: Vec::with_capacity(replica_count),
            },
        );
    }
    pub(crate) fn take(
        &mut self,
        offset: &u64,
    ) -> Option<ConsensusVoting<Sender<ConsensusClientResponse>>> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<u64, ConsensusVoting<Sender<ConsensusClientResponse>>>);
