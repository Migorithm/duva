use super::*;

use crate::domains::cluster_actors::consensus::election::ElectionState;
use crate::domains::cluster_actors::consensus::election::ElectionVoting;
use crate::domains::peers::command::ElectionVote;
use crate::domains::peers::command::RequestVote;

#[tokio::test]
async fn test_run_for_election_transitions_to_candidate_and_sends_request_votes() {
    // GIVEN: A follower actor with a couple of replica peers

    let mut actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    let initial_term = actor.replication.term;
    let (fakebuf1, _) = actor.test_add_peer(8001, None, false);
    let (fakebuf2, _) = actor.test_add_peer(8002, None, false);

    // WHEN: The actor runs for election
    actor.run_for_election().await;

    // THEN: Actor's state should be Candidate
    assert_eq!(actor.replication.role, ReplicationRole::Follower);
    assert_eq!(actor.replication.term, initial_term + 1);
    assert!(matches!(
        actor.replication.election_state,
        ElectionState::Candidate { voting: Some(ElectionVoting { cnt: 1, replica_count: 2 }) }
    ));

    // THEN: RequestVote messages should be sent to peers
    let expected_request_vote = RequestVote {
        term: initial_term + 1,
        candidate_id: actor.replication.self_identifier(),
        last_log_index: actor.logger.last_log_index,
        last_log_term: actor.logger.last_log_term,
    };

    assert_expected_queryio(&fakebuf1, expected_request_vote.clone()).await;
    assert_expected_queryio(&fakebuf2, expected_request_vote).await;
}

#[tokio::test]
async fn test_run_for_election_no_replicas() {
    // GIVEN: A follower actor with no replicas

    let mut actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    let initial_term = actor.replication.term;

    // WHEN: The actor runs for election
    actor.run_for_election().await;

    // THEN: Actor's state should be Candidate, term incremented, voted for self
    assert!(matches!(actor.replication.role, ReplicationRole::Follower));

    assert_eq!(actor.replication.term, initial_term + 1);

    assert!(matches!(
        actor.replication.election_state,
        ElectionState::Candidate { voting: Some(ElectionVoting { cnt: 1, replica_count: 0 }) }
    ));
}

#[tokio::test]
async fn test_vote_election_grant_vote() {
    // GIVEN: A follower actor
    let mut follower_actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    let initial_term = follower_actor.replication.term;

    let (candidate_fake_buf, candidate_id) = follower_actor.test_add_peer(8011, None, false);

    // Candidate's log is up-to-date or newer, and term is higher
    let request_vote = RequestVote {
        term: initial_term + 1,
        candidate_id: candidate_id.clone(),
        last_log_index: 1, // Same log index
        last_log_term: 1,  // Same log term
    };

    // WHEN
    follower_actor.vote_election(request_vote.clone()).await;

    // THEN: Follower should grant the vote and update its state
    assert_eq!(follower_actor.replication.term, initial_term + 1);
    assert!(matches!(
        follower_actor.replication.election_state,
        ElectionState::Follower { voted_for: Some(..) }
    ));
    assert_eq!(follower_actor.replication.role, ReplicationRole::Follower); // Stays follower

    // Check message sent to candidate
    assert_expected_queryio(
        &candidate_fake_buf,
        ElectionVote { term: initial_term + 1, vote_granted: true },
    )
    .await;
}

#[tokio::test]
async fn test_vote_election_deny_vote_older_log() {
    // GIVEN: A follower actor
    let mut follower_actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    let initial_term = follower_actor.replication.term;

    follower_actor
        .logger
        .follower_write_entries(vec![WriteOperation {
            log_index: initial_term + 2,
            term: initial_term,
            request: WriteRequest::Set { key: "k".into(), value: "v".into(), expires_at: None },
            session_req: None,
        }])
        .unwrap(); // Follower log: idx 2, term 2

    let (candidate_fake_buf, candidate_id) = follower_actor.test_add_peer(8031, None, false);

    let request_vote = RequestVote {
        // Candidate log: idx 1, term 2 (older)
        term: initial_term + 1, // Candidate has higher term
        candidate_id: candidate_id.clone(),
        last_log_index: 1,
        last_log_term: initial_term,
    };

    //WHEN
    follower_actor.vote_election(request_vote.clone()).await;

    //THEN
    assert!(matches!(
        follower_actor.replication.election_state,
        ElectionState::Follower { voted_for: None }
    ));

    assert_expected_queryio(
        &candidate_fake_buf,
        ElectionVote { term: initial_term, vote_granted: false },
    )
    .await;
}

#[tokio::test]
async fn test_vote_election_deny_vote_lower_candidate_term() {
    let follower_term = 3;
    let mut follower_actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    follower_actor.replication.term = follower_term;

    let (candidate_fake_buf, candidate_id) = follower_actor.test_add_peer(8031, None, false);

    let request_vote = RequestVote {
        term: follower_term - 1, // Candidate term is lower
        candidate_id: candidate_id.clone(),
        last_log_index: 1,
        last_log_term: follower_term - 1,
    };

    follower_actor.vote_election(request_vote.clone()).await;

    assert_eq!(follower_actor.replication.term, follower_term); // Term does not change

    assert_expected_queryio(
        &candidate_fake_buf,
        ElectionVote { term: follower_term, vote_granted: false },
    )
    .await;
}

#[tokio::test]
async fn test_receive_election_vote_candidate_wins_election() {
    // GIVEN: A candidate actor needing one more vote to win (2 replicas + self = 3 total, needs 2 votes)
    let candidate_term = 3;
    let mut candidate_actor = Helper::cluster_actor(ReplicationRole::Follower).await;

    // Manually set up as candidate that has run for election
    candidate_actor.replication.term = candidate_term;

    let voting = ElectionVoting::new(2);

    candidate_actor.replication.election_state = ElectionState::Candidate { voting: Some(voting) };

    // Add a mock replica to send initial heartbeat to
    let (replica1_fake_buf, _) = candidate_actor.test_add_peer(8051, None, false);

    let election_vote = ElectionVote { term: candidate_term, vote_granted: true };

    // WHEN: Candidate receives the winning vote
    candidate_actor.receive_election_vote(election_vote).await;

    // THEN: Candidate should become Leader
    assert!(candidate_actor.replication.is_leader());
    assert_eq!(candidate_actor.replication.role, ReplicationRole::Leader);
    assert_eq!(candidate_actor.replication.term, candidate_term); // Term remains the same as election term

    // THEN: Initial heartbeat should be sent to the replica
    // The receive_election_vote calls become_leader, which sends an AppendEntriesRPC
    let hb = HeartBeat {
        term: candidate_term,
        from: candidate_actor.replication.self_identifier(),
        replid: candidate_actor.replication.replid.clone(),
        ..Default::default()
    };
    assert_expected_queryio(&replica1_fake_buf, QueryIO::AppendEntriesRPC(hb.clone())).await;

    assert_expected_queryio(
        &replica1_fake_buf,
        QueryIO::ClusterHeartBeat(hb.set_hashring(candidate_actor.hash_ring.clone())),
    )
    .await;
}

#[tokio::test]
async fn test_receive_election_vote_candidate_gets_vote_not_enough_to_win() {
    let candidate_term = 3;
    let mut candidate_actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    candidate_actor.replication.term = candidate_term;

    candidate_actor.replication.election_state =
        ElectionState::Candidate { voting: Some(ElectionVoting::new(5)) };

    let election_vote = ElectionVote { term: candidate_term, vote_granted: true };

    candidate_actor.receive_election_vote(election_vote).await;

    assert_eq!(candidate_actor.replication.role, ReplicationRole::Follower); // Stays follower

    if let ElectionState::Candidate { voting } = candidate_actor.replication.election_state {
        assert_eq!(voting.unwrap().cnt, 2);
    } else {
        panic!("Expected candidate state");
    }
}
