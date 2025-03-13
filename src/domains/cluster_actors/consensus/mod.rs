use std::collections::HashMap;

use tokio::sync::oneshot::Sender;

use crate::{
    domains::{append_only_files::log::LogIndex, peers::identifier::PeerIdentifier},
    make_smart_pointer,
};

use super::commands::{RequestVoteReply, WriteConsensusResponse};

#[derive(Debug)]
pub struct ConsensusVoting<T> {
    callback: Sender<T>,
    vote_count: u8,
    required_votes: u8,
}
impl<T> ConsensusVoting<T> {
    pub fn increase_vote(&mut self) {
        self.vote_count += 1;
    }
}

impl ConsensusVoting<WriteConsensusResponse> {
    pub fn maybe_not_finished(self, log_index: LogIndex) -> Option<Self> {
        if self.vote_count >= self.required_votes {
            let _ = self.callback.send(WriteConsensusResponse::LogIndex(Some(log_index)));
            None
        } else {
            Some(self)
        }
    }
}

impl ConsensusVoting<()> {
    pub fn maybe_not_finished(mut self, granted: bool) -> Option<Self> {
        if granted {
            println!("111");
            self.increase_vote();
            if self.vote_count >= self.required_votes {
                let _ = self.callback.send(());
                None
            } else {
                Some(self)
            }
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

    pub(crate) fn to_candidate(
        &mut self,
        replica_count: usize,
        callback: tokio::sync::oneshot::Sender<()>,
    ) {
        *self = ElectionState::Candidate {
            voting: Some(ConsensusVoting {
                callback,
                vote_count: 1,
                required_votes: get_required_votes(replica_count),
            }),
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

    pub(crate) fn may_become_leader(&mut self, request_vote_reply: RequestVoteReply) -> bool {
        match self {
            ElectionState::Candidate { voting } => {
                let Some(current_voting) = voting.take() else {
                    return false;
                };

                if let Some(unfinished_voting) =
                    current_voting.maybe_not_finished(request_vote_reply.vote_granted)
                {
                    *voting = Some(unfinished_voting);

                    return false;
                } else {
                    *self = ElectionState::Leader;
                    return true;
                }
            },
            _ => false,
        }
    }
}

fn get_required_votes(replica_count: usize) -> u8 {
    ((replica_count as f64 + 1.0) / 2.0).ceil() as u8
}
