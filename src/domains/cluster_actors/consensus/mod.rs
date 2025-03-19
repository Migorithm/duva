pub mod voting;
use super::commands::WriteConsensusResponse;
use crate::make_smart_pointer;

use std::collections::HashMap;
use tokio::sync::oneshot::Sender;
use voting::ConsensusVoting;

#[derive(Default, Debug)]
pub struct LogConsensusTracker(
    pub(crate) HashMap<u64, ConsensusVoting<Sender<WriteConsensusResponse>>>,
);
impl LogConsensusTracker {
    pub(crate) fn add(
        &mut self,
        key: u64,
        value: Sender<WriteConsensusResponse>,
        replica_count: usize,
    ) {
        self.0
            .insert(key, ConsensusVoting { callback: value, pos_vt: 0, neg_vt: 0, replica_count });
    }
    pub(crate) fn take(
        &mut self,
        offset: &u64,
    ) -> Option<ConsensusVoting<Sender<WriteConsensusResponse>>> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<u64, ConsensusVoting<Sender<WriteConsensusResponse>>>);
