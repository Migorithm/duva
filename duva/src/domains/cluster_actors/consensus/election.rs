use std::collections::HashSet;

use crate::domains::peers::identifier::PeerIdentifier;

#[derive(Debug, Clone, Default)]
pub(crate) struct ElectionVotes {
    pub(crate) replica_count: u8,
    pub(crate) votes: HashSet<PeerIdentifier>,
}

impl ElectionVotes {
    pub(crate) fn new(replica_count: u8, self_id: PeerIdentifier) -> Self {
        let mut voters = HashSet::new();
        voters.insert(self_id);
        Self { replica_count, votes: voters }
    }

    fn get_required_votes(&self) -> usize {
        ((self.replica_count + 1).div_ceil(2)) as usize
    }

    pub(crate) fn record_vote(&mut self, voter_id: PeerIdentifier) -> bool {
        self.votes.insert(voter_id)
    }

    pub(crate) fn has_majority(&self) -> bool {
        self.votes.len() >= self.get_required_votes()
    }
    pub(crate) fn is_votable(&self, candidate_id: &PeerIdentifier) -> bool {
        self.votes.is_empty() || (self.votes.len() == 1 && self.votes.contains(candidate_id))
    }
}

#[test]
fn test_get_required_votes() {
    let ev = ElectionVotes::new(0, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 1);

    let ev = ElectionVotes::new(1, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 1);

    let ev = ElectionVotes::new(2, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 2);

    let ev = ElectionVotes::new(3, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 2);

    let ev = ElectionVotes::new(4, PeerIdentifier("peer1".into()));
    assert_eq!(ev.get_required_votes(), 3);
}
