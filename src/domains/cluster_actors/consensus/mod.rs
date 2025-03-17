pub mod voting;
use super::commands::WriteConsensusResponse;
use crate::{domains::append_only_files::log::LogIndex, make_smart_pointer};

use std::collections::HashMap;
use tokio::sync::oneshot::Sender;
use voting::ConsensusVoting;

#[derive(Default, Debug)]
pub struct LogConsensusTracker(HashMap<LogIndex, ConsensusVoting<Sender<WriteConsensusResponse>>>);
impl LogConsensusTracker {
    pub fn add(
        &mut self,
        key: LogIndex,
        value: Sender<WriteConsensusResponse>,
        replica_count: usize,
    ) {
        self.0
            .insert(key, ConsensusVoting { callback: value, pos_vt: 0, neg_vt: 0, replica_count });
    }
    pub fn take(
        &mut self,
        offset: &LogIndex,
    ) -> Option<ConsensusVoting<Sender<WriteConsensusResponse>>> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<LogIndex, ConsensusVoting<Sender<WriteConsensusResponse>>>);
