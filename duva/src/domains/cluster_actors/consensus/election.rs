use std::collections::HashSet;

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
            | ReplicationRole::Leader => ElectionState::Leader,
            | ReplicationRole::Follower => ElectionState::Follower { voted_for: None },
        }
    }

    pub(crate) fn is_votable(&self, candidate_id: &PeerIdentifier) -> bool {
        match self {
            | ElectionState::Follower { voted_for } => match voted_for {
                | None => true,
                | Some(id) => id == candidate_id,
            },
            | _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ElectionVoting {
    pub(crate) replica_count: u8,
    pub(crate) voters: HashSet<PeerIdentifier>,
}

impl ElectionVoting {
    pub(crate) fn new(replica_count: u8, self_id: PeerIdentifier) -> Self {
        let mut voters = HashSet::new();
        voters.insert(self_id);
        Self { replica_count, voters }
    }

    fn get_required_votes(&self) -> usize {
        ((self.replica_count + 1).div_ceil(2)) as usize
    }

    pub(crate) fn record_vote(&mut self, voter_id: PeerIdentifier) -> bool {
        self.voters.insert(voter_id)
    }

    pub(crate) fn has_majority(&self) -> bool {
        self.voters.len() >= self.get_required_votes()
    }
}

#[test]
fn test_get_required_votes() {
    let ev = ElectionVoting::new(0, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 1);

    let ev = ElectionVoting::new(1, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 1);

    let ev = ElectionVoting::new(2, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 2);

    let ev = ElectionVoting::new(3, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 2);

    let ev = ElectionVoting::new(4, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 3);
}
