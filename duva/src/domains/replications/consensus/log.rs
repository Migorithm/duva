use crate::{
    domains::{cluster_actors::ConsensusClientResponse, peers::identifier::PeerIdentifier},
    make_smart_pointer,
    presentation::clients::request::ClientReq,
    types::Callback,
};
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct LogConsensusTracker(pub(crate) HashMap<u64, LogConsensusVoting>);

make_smart_pointer!(LogConsensusTracker, HashMap<u64, LogConsensusVoting>);

#[derive(Debug)]
pub struct LogConsensusVoting {
    pub(crate) voters: Vec<PeerIdentifier>,
    pub(crate) callback: Callback<ConsensusClientResponse>,
    pub(crate) cnt: u8,
    pub(crate) session_req: Option<ClientReq>,
}
impl LogConsensusVoting {
    pub(crate) fn new(
        callback: Callback<ConsensusClientResponse>,
        replica_count: usize,
        session_req: Option<ClientReq>,
    ) -> Self {
        Self { callback, cnt: 1, voters: Vec::with_capacity(replica_count), session_req }
    }

    pub(crate) fn increase_vote(&mut self, voter: PeerIdentifier) {
        self.cnt += 1;
        self.voters.push(voter);
    }

    pub(crate) fn get_required_votes(&self) -> u8 {
        let total_nodes = self.voters.capacity() as u8 + 1; // +1 for the leader
        (total_nodes + 1).div_ceil(2)
    }

    pub(crate) fn is_eligible_voter(&self, voter: &PeerIdentifier) -> bool {
        !self.voters.contains(voter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_required_votes() {
        let test_cases = vec![
            (0, 1), // 0 followers: total_nodes = 1 (leader), required = 1
            (1, 2), // 1 follower: total_nodes = 2, required = 2
            (2, 2), // 2 followers: total_nodes = 3, required = 2
            (3, 3), // 3 followers: total_nodes = 4, required = 3
            (4, 3), // 4 followers: total_nodes = 5, required = 3
            (5, 4), // 5 followers: total_nodes = 6, required = 4
        ];

        for (follower_count, expected_votes) in test_cases {
            let voting = LogConsensusVoting::new(Callback::create().0.into(), follower_count, None);
            assert_eq!(voting.get_required_votes(), expected_votes);
        }
    }

    #[test]
    fn test_get_required_votes_edge_cases() {
        // Test with a large but safe number of followers
        let follower_count = 100;
        let voting = LogConsensusVoting::new(Callback::create().0.into(), follower_count, None);
        let total_nodes = follower_count as u8 + 1;
        let expected_votes = (total_nodes + 1).div_ceil(2);
        assert_eq!(voting.get_required_votes(), expected_votes);
    }
}
