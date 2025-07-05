use std::collections::HashMap;

use crate::{
    domains::{
        cluster_actors::{ConsensusClientResponse, ConsensusRequest, SessionRequest},
        peers::identifier::PeerIdentifier,
    },
    make_smart_pointer,
    types::Callback,
};
pub(crate) type ReplicationVote = Callback<ConsensusClientResponse>;

#[derive(Default, Debug)]
pub struct LogConsensusTracker(pub(crate) HashMap<u64, LogConsensusVoting>);
impl LogConsensusTracker {
    pub(crate) fn add(&mut self, key: u64, req: ConsensusRequest, replica_count: usize) {
        self.insert(key, LogConsensusVoting::new(req.callback, replica_count, req.session_req));
    }
}
make_smart_pointer!(LogConsensusTracker, HashMap<u64, LogConsensusVoting>);

#[derive(Debug)]
pub struct LogConsensusVoting {
    pub(crate) voters: Vec<PeerIdentifier>,
    pub(crate) callback: ReplicationVote,
    pub(crate) cnt: u8,
    pub(crate) session_req: Option<SessionRequest>,
}
impl LogConsensusVoting {
    fn new(
        callback: ReplicationVote,
        replica_count: usize,
        session_req: Option<SessionRequest>,
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

    pub(crate) fn votable(&self, voter: &PeerIdentifier) -> bool {
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
            let voting = LogConsensusVoting::new(
                tokio::sync::oneshot::channel().0.into(),
                follower_count,
                None,
            );
            assert_eq!(voting.get_required_votes(), expected_votes);
        }
    }

    #[test]
    fn test_get_required_votes_edge_cases() {
        // Test with a large but safe number of followers
        let follower_count = 100;
        let voting =
            LogConsensusVoting::new(tokio::sync::oneshot::channel().0.into(), follower_count, None);
        let total_nodes = follower_count as u8 + 1;
        let expected_votes = (total_nodes + 1).div_ceil(2);
        assert_eq!(voting.get_required_votes(), expected_votes);
    }
}
