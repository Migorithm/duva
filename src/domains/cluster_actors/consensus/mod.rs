pub mod enums;
use super::commands::{RequestVoteReply, WriteConsensusResponse};
use crate::{
    domains::{append_only_files::log::LogIndex, peers::identifier::PeerIdentifier},
    make_smart_pointer,
};

use enums::ConsensusState;
use std::collections::HashMap;
use tokio::sync::oneshot::Sender;

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    callback: Sender<T>,
    pos_vt: u8,
    neg_vt: u8,
    replica_count: usize,
}
impl<T> ConsensusVoting<T> {
    pub fn increase_vote(&mut self) {
        self.pos_vt += 1;
    }

    fn get_required_votes(&self) -> u8 {
        ((self.replica_count as f64 + 1.0) / 2.0).ceil() as u8
    }
}

impl ConsensusVoting<WriteConsensusResponse> {
    pub fn maybe_not_finished(self, log_index: LogIndex) -> Option<Self> {
        if self.pos_vt >= self.get_required_votes() {
            let _ = self.callback.send(WriteConsensusResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}

impl ConsensusVoting<bool> {
    pub fn maybe_not_finished(mut self, granted: bool) -> Result<bool, Self> {
        if granted {
            self.increase_vote();
        } else {
            self.neg_vt += 1;
        }

        let required_count = self.get_required_votes();
        if self.pos_vt >= required_count {
            let _ = self.callback.send(true);
            Ok(true)
        } else if self.neg_vt >= required_count {
            let _ = self.callback.send(true);
            Ok(false)
        } else {
            Err(self)
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
        self.0
            .insert(key, ConsensusVoting { callback: value, pos_vt: 0, neg_vt: 0, replica_count });
    }
    pub fn take(&mut self, offset: &LogIndex) -> Option<ConsensusVoting<WriteConsensusResponse>> {
        self.0.remove(offset)
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<LogIndex, ConsensusVoting<WriteConsensusResponse>>);

#[derive(Debug)]
pub enum ElectionState {
    Candidate { voting: Option<ConsensusVoting<bool>> },
    Follower { voted_for: Option<PeerIdentifier> },
    Leader,
}

impl ElectionState {
    pub(crate) fn new(role: &str) -> Self {
        match role {
            "leader" => ElectionState::Leader,
            _ => ElectionState::Follower { voted_for: None },
        }
    }

    pub(crate) fn to_candidate(
        &mut self,
        replica_count: usize,
        callback: tokio::sync::oneshot::Sender<bool>,
    ) {
        *self = ElectionState::Candidate {
            voting: Some(ConsensusVoting { callback, pos_vt: 1, neg_vt: 0, replica_count }),
        };
    }

    pub(crate) fn is_votable(&self, candidiate_id: &PeerIdentifier) -> bool {
        match self {
            ElectionState::Follower { voted_for } => {
                if voted_for.is_none() || voted_for.as_ref() == Some(candidiate_id) {
                    true
                } else {
                    false
                }
            },
            _ => false,
        }
    }

    pub(crate) fn may_become_leader(
        &mut self,
        request_vote_reply: RequestVoteReply,
    ) -> ConsensusState {
        match self {
            ElectionState::Candidate { voting } => {
                let Some(current_voting) = voting.take() else {
                    return ConsensusState::NotYetFinished;
                };

                match current_voting.maybe_not_finished(request_vote_reply.vote_granted) {
                    Ok(become_leader) => {
                        if become_leader {
                            *self = ElectionState::Leader;
                            return ConsensusState::Succeeded;
                        } else {
                            *self = ElectionState::Follower { voted_for: None };
                            return ConsensusState::Failed;
                        }
                    },
                    Err(unfinished_voting) => {
                        *voting = Some(unfinished_voting);
                        return ConsensusState::NotYetFinished;
                    },
                }
            },
            _ => ConsensusState::NotYetFinished,
        }
    }
}
