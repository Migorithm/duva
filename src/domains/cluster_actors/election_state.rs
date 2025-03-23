use crate::domains::peers::identifier::PeerIdentifier;

#[derive(Debug, Clone)]
pub(crate) enum ElectionState {
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
        *self = ElectionState::Candidate { voting: Some(ElectionVoting { cnt: 1, replica_count }) };
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

    pub(crate) fn should_become_leader(&mut self, granted: bool) -> Option<()> {
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
pub(crate) struct ElectionVoting {
    pub(crate) cnt: u8,

    pub(crate) replica_count: usize,
}

impl ElectionVoting {
    pub(crate) fn increase_vote(&mut self) {
        self.cnt += 1;
    }

    fn get_required_votes(&self) -> u8 {
        ((self.replica_count as f64 + 1.0) / 2.0).ceil() as u8
    }
    pub(crate) fn voting_maybe_finished(mut self, granted: bool) -> Result<(), Self> {
        if granted {
            self.increase_vote();
        }

        let required_count = self.get_required_votes();
        if self.cnt >= required_count {
            return Ok(());
        }
        Err(self)
    }
}
