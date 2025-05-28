use super::*;
use crate::domains::cluster_actors::consensus::ElectionState;
use crate::domains::cluster_actors::consensus::ElectionVoting;
use crate::domains::peers::command::RequestVote;

//TODO BUG! leader shouldn't start election again
#[tokio::test]
async fn test_run_for_election_transitions_to_candidate_and_sends_request_votes() {
    // GIVEN: A follower actor with a couple of replica peers

    let mut actor = cluster_actor_create_helper().await;
    let initial_term = actor.replication.term;
    let fakebuf1 = add_replica_helper(&mut actor, 8001);
    let fakebug2 = add_replica_helper(&mut actor, 8002);

    // WHEN: The actor runs for election
    actor.run_for_election().await;

    // THEN: Actor's state should be Candidate
    assert_eq!(actor.replication.role, ReplicationRole::Leader);
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

    let msg1 = fakebuf1.0.lock().await.pop_front().unwrap();
    if let QueryIO::RequestVote(rv) = msg1 {
        assert_eq!(rv, expected_request_vote);
    } else {
        panic!("Expected RequestVote, got {:?}", msg1);
    }

    let msg2 = fakebug2.0.lock().await.pop_front().unwrap();
    if let QueryIO::RequestVote(rv) = msg2 {
        assert_eq!(rv, expected_request_vote);
    } else {
        panic!("Expected RequestVote, got {:?}", msg2);
    }
}

#[tokio::test]
async fn test_run_for_election_no_replicas() {
    // GIVEN: A follower actor with no replicas

    let mut actor = cluster_actor_create_helper().await;
    let initial_term = actor.replication.term;
    actor.replication.role = ReplicationRole::Follower;

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
