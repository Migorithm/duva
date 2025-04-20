use crate::domains::{
    cluster_actors::replication::ReplicationRole, peers::identifier::PeerIdentifier,
};

#[derive(Debug, Clone)]
pub(crate) enum ElectionState {
    Candidate { voting: Option<ElectionVoting> },
    Follower { voted_for: Option<PeerIdentifier> },
    Leader,
}

impl ElectionState {
    pub(crate) fn new(role: &ReplicationRole) -> Self {
        match role {
            ReplicationRole::Leader => ElectionState::Leader,
            ReplicationRole::Follower => ElectionState::Follower { voted_for: None },
        }
    }
    pub(crate) fn become_leader(&mut self) {
        *self = ElectionState::Leader;
    }
    pub(crate) fn become_candidate(&mut self, replica_count: u8) {
        *self = ElectionState::Candidate { voting: Some(ElectionVoting { cnt: 0, replica_count }) };
    }

    pub(crate) fn is_votable(&self, candidiate_id: &PeerIdentifier) -> bool {
        match self {
            ElectionState::Follower { voted_for } => {
                voted_for.is_none() || voted_for.as_ref() == Some(candidiate_id)
            },
            _ => false,
        }
    }

    pub(crate) fn may_become_leader(&mut self) -> bool {
        let ElectionState::Candidate { voting } = self else { return false };
        // Try to take ownership of the current voting state
        let Some(current_voting) = voting.take() else {
            return false;
        };
        // Process the vote with granted (true/false)
        *voting = current_voting.voting_if_unfinished();
        voting.is_none()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ElectionVoting {
    pub(crate) cnt: u8,
    pub(crate) replica_count: u8,
}

impl ElectionVoting {
    fn get_required_votes(&self) -> u8 {
        (self.replica_count + 1).div_ceil(2)
    }
    pub(crate) fn voting_if_unfinished(mut self) -> Option<Self> {
        self.cnt += 1;

        let required_count = self.get_required_votes();
        if self.cnt >= required_count {
            return None;
        }
        Some(self)
    }
}

#[test]
fn test_get_required_votes() {
    let ev = ElectionVoting { cnt: 0, replica_count: 0 };
    assert_eq!(ev.get_required_votes(), 1);

    let ev = ElectionVoting { cnt: 0, replica_count: 1 };
    assert_eq!(ev.get_required_votes(), 1);

    let ev = ElectionVoting { cnt: 0, replica_count: 2 };
    assert_eq!(ev.get_required_votes(), 2);

    let ev = ElectionVoting { cnt: 0, replica_count: 3 };
    assert_eq!(ev.get_required_votes(), 2);

    let ev = ElectionVoting { cnt: 0, replica_count: 4 };
    assert_eq!(ev.get_required_votes(), 3);
}
