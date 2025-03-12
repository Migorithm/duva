use std::collections::HashMap;

use tokio::sync::oneshot::Sender;

use crate::{
    domains::{append_only_files::log::LogIndex, peers::identifier::PeerIdentifier},
    make_smart_pointer,
};

use super::{commands::WriteConsensusResponse, replication::ReplicationInfo};

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    callback: Sender<T>,
    vote_count: u8,
    required_votes: u8,
}
impl ConsensusVoting<WriteConsensusResponse> {
    pub fn apply_vote(&mut self) {
        self.vote_count += 1;
    }

    pub fn maybe_not_finished(self, log_index: LogIndex) -> Option<Self> {
        if self.vote_count >= self.required_votes {
            let _ = self.callback.send(WriteConsensusResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}

#[derive(Default, Debug)]
pub struct LogConsensusTracker(HashMap<LogIndex, ConsensusVoting<WriteConsensusResponse>>);
impl LogConsensusTracker {
    pub fn add(
        &mut self,
        key: LogIndex,
        value: Sender<WriteConsensusResponse>,
        replica_count: usize,
    ) {
        self.0.insert(
            key,
            ConsensusVoting {
                callback: value,
                vote_count: 0,
                required_votes: get_required_votes(replica_count),
            },
        );
    }
    pub fn take(&mut self, offset: &LogIndex) -> Option<ConsensusVoting<WriteConsensusResponse>> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<LogIndex, ConsensusVoting<WriteConsensusResponse>>);

#[derive(Debug)]
pub enum ElectionState {
    Candidate { voted_for: PeerIdentifier, voting: ConsensusVoting<()> },
    Follower,
    Leader,
}
impl ElectionState {
    pub(crate) fn new(role: &str) -> Self {
        match role {
            "leader" => ElectionState::Leader,
            _ => ElectionState::Follower,
        }
    }

    pub(crate) fn run_candidate(
        &mut self,
        replication: &ReplicationInfo,
        replica_count: usize,
        callback: tokio::sync::oneshot::Sender<()>,
    ) {
        *self = ElectionState::Candidate {
            voted_for: replication.self_identifier(),
            voting: ConsensusVoting {
                callback,
                vote_count: 1,
                required_votes: get_required_votes(replica_count),
            },
        };
    }
}

fn get_required_votes(replica_count: usize) -> u8 {
    ((replica_count as f64 + 1.0) / 2.0).ceil() as u8
}
