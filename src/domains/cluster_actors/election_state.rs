use crate::domains::peers::identifier::PeerIdentifier;

use super::{commands::RequestVoteReply, consensus::enums::ConsensusState};

#[derive(Debug, Clone)]
pub enum ElectionState {
    Candidate { voting: Option<ElectionVoting> },
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
            voting: Some(ElectionVoting { pos_vt: 1, neg_vt: 0, replica_count }),
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

    pub(crate) fn may_become_leader(&mut self, granted: bool) -> ConsensusState {
        match self {
            ElectionState::Candidate { voting } => {
                let Some(current_voting) = voting.take() else {
                    return ConsensusState::NotYetFinished;
                };

                match current_voting.maybe_not_finished(granted) {
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

#[derive(Debug, Clone)]
pub struct ElectionVoting {
    pub(crate) pos_vt: u8,
    pub(crate) neg_vt: u8,
    pub(crate) replica_count: usize,
}

impl ElectionVoting {
    pub(crate) fn increase_vote(&mut self) {
        self.pos_vt += 1;
    }

    fn get_required_votes(&self) -> u8 {
        ((self.replica_count as f64 + 1.0) / 2.0).ceil() as u8
    }
    pub(crate) fn maybe_not_finished(mut self, granted: bool) -> Result<bool, Self> {
        if granted {
            self.increase_vote();
        } else {
            self.neg_vt += 1;
        }

        let required_count = self.get_required_votes();
        if self.pos_vt >= required_count {
            Ok(true)
        } else if self.neg_vt >= required_count {
            Ok(false)
        } else {
            Err(self)
        }
    }
}
