pub mod enums;
pub mod voting;
use super::commands::{RequestVoteReply, WriteConsensusResponse};
use crate::{
    domains::{append_only_files::log::LogIndex, peers::identifier::PeerIdentifier},
    make_smart_pointer,
};

use enums::ConsensusState;
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

#[derive(Debug)]
pub enum ElectionState {
    Candidate { voting: Option<ConsensusVoting<()>> },
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

    pub(crate) fn to_candidate(&mut self, replica_count: usize) {
        *self = ElectionState::Candidate {
            voting: Some(ConsensusVoting { pos_vt: 1, neg_vt: 0, replica_count, callback: () }),
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
