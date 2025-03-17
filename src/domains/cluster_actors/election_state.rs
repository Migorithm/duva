use crate::domains::peers::identifier::PeerIdentifier;

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
    pub(crate) fn become_leader(&mut self) {
        *self = ElectionState::Leader;
    }
    pub(crate) fn become_candidate(&mut self, replica_count: usize) {
        *self = ElectionState::Candidate {
            voting: Some(ElectionVoting { pos_vt: 1, neg_vt: 0, replica_count }),
        };
    }
    pub(crate) fn become_follower(&mut self, candidate_id: Option<PeerIdentifier>) {
        *self = ElectionState::Follower { voted_for: candidate_id };
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

    pub(crate) fn should_become_leader(&mut self, granted: bool) -> Option<bool> {
        if let ElectionState::Candidate { voting } = self {
            // Try to take ownership of the current voting state
            if let Some(current_voting) = voting.take() {
                // Process the vote with granted (true/false)
                match current_voting.voting_maybe_finished(granted) {
                    Ok(become_leader) => return Some(become_leader),
                    Err(unfinished_voting) => {
                        // Put the updated voting state back
                        *voting = Some(unfinished_voting);
                    },
                }
            }
        }
        return None;
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
    pub(crate) fn voting_maybe_finished(mut self, granted: bool) -> Result<bool, Self> {
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
