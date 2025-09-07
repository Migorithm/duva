use super::*;

#[test]
fn logger_create_entries_from_lowest() {
    // GIVEN
    let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);

    let test_logs = vec![
        Helper::write(1, 0, "foo", "bar"),
        Helper::write(2, 0, "foo2", "bar"),
        Helper::write(3, 0, "foo3", "bar"),
    ];
    logger.follower_write_entries(test_logs.clone()).unwrap();

    // WHEN
    const LOWEST_FOLLOWER_COMMIT_INDEX: u64 = 2;
    let mut repl_state = ReplicationState::new(
        ReplicationId::Key("master".into()),
        ReplicationRole::Leader,
        "localhost",
        8080,
        logger,
    );
    repl_state.logger.con_idx.store(LOWEST_FOLLOWER_COMMIT_INDEX, Ordering::Release);

    let log = &LogEntry::Set { key: "foo4".into(), value: "bar".into(), expires_at: None };
    repl_state.logger.write_single_entry(log, repl_state.term, None).unwrap();

    let logs = repl_state.logger.list_append_log_entries(Some(LOWEST_FOLLOWER_COMMIT_INDEX));

    // THEN
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].log_index, 3);
    assert_eq!(logs[1].log_index, 4);
    assert_eq!(repl_state.logger.last_log_index, 4);
}

#[tokio::test]
async fn test_generate_follower_entries() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let replid = cluster_actor.replication.replid.clone();
    let (cluster_sender, _) = ClusterActorQueue::new(100);
    let follower_buffs = (0..5).map(|_| FakeReadWrite::new()).collect::<Vec<_>>();

    Helper::cluster_member(
        &mut cluster_actor,
        follower_buffs.clone(),
        cluster_sender.clone(),
        3,
        Some(replid.clone()),
    );

    let test_logs = vec![
        Helper::write(1, 0, "foo", "bar"),
        Helper::write(2, 0, "foo2", "bar"),
        Helper::write(3, 0, "foo3", "bar"),
    ];

    cluster_actor.replication.logger.con_idx.store(3, Ordering::Release);

    cluster_actor.replication.logger.follower_write_entries(test_logs).unwrap();

    //WHEN
    // *add lagged followers with its commit index being 1
    let follower_buffs = (5..7).map(|_| FakeReadWrite::new()).collect::<Vec<_>>();
    Helper::cluster_member(&mut cluster_actor, follower_buffs, cluster_sender, 1, Some(replid));

    // * add new log - this must create entries that are greater than 3

    cluster_actor
        .replication
        .logger
        .write_single_entry(
            &LogEntry::Set { key: "foo4".into(), value: "bar".into(), expires_at: None },
            cluster_actor.replication.term,
            None,
        )
        .unwrap();

    let entries = cluster_actor.iter_follower_append_entries().collect::<Vec<_>>();

    // * for old followers must have 1 entry
    assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 1).count(), 5);
    // * for lagged followers must have 3 entries
    assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 3).count(), 2)
}

#[tokio::test]
async fn follower_cluster_actor_replicate_log() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    // WHEN - term
    let heartbeat = Helper::heartbeat(
        0,
        Some(0),
        vec![Helper::write(1, 0, "foo", "bar"), Helper::write(2, 0, "foo2", "bar")],
    );

    cluster_actor.replicate(heartbeat).await;

    // THEN
    assert_eq!(cluster_actor.replication.logger.con_idx.load(Ordering::Relaxed), 0);
    assert_eq!(cluster_actor.replication.logger.last_log_index, 2);
    let logs = cluster_actor.replication.logger.range(0, 2);
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].log_index, 1);
    assert_eq!(logs[1].log_index, 2);
    assert_eq!(
        logs[0].request,
        LogEntry::Set { key: "foo".into(), value: "bar".into(), expires_at: None }
    );
    assert_eq!(
        logs[1].request,
        LogEntry::Set { key: "foo2".into(), value: "bar".into(), expires_at: None }
    );
}

#[tokio::test]
async fn follower_cluster_actor_sessionless_replicate_state() {
    // GIVEN
    let (cache_handler, mut receiver) = tokio::sync::mpsc::channel(100);
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Follower).await;

    let heartbeat = Helper::heartbeat(
        0,
        Some(1),
        vec![Helper::write(1, 0, "foo", "bar"), Helper::write(2, 0, "foo2", "bar")],
    );

    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(cache_handler)] };
    cluster_actor.cache_manager = cache_manager.clone();
    cluster_actor.replicate(heartbeat.clone()).await;

    // WHEN - commit until 2
    let task = tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            match message {
                | CacheCommand::Set { cache_entry } => {
                    let (key, value) = cache_entry.destructure();
                    assert_eq!(value, "bar");
                    if key == "foo2" {
                        break;
                    }
                },
                | _ => continue,
            }
        }
    });
    let heartbeat = Helper::heartbeat(0, Some(2), vec![]);
    cluster_actor.replicate(heartbeat).await;

    // THEN
    task.await.unwrap();
    assert_eq!(cluster_actor.replication.logger.con_idx.load(Ordering::Relaxed), 2);
    assert_eq!(cluster_actor.replication.logger.last_log_index, 2);
}

#[tokio::test]
async fn replicate_stores_only_latest_session_per_client() {
    // GIVEN

    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Follower).await;

    let target_client = uuid::Uuid::now_v7().to_string();
    let session1 = SessionRequest::new(1, uuid::Uuid::now_v7().to_string());
    let session2 = SessionRequest::new(1, target_client.clone());
    // ! For the same client, hold only one request
    let session3 = SessionRequest::new(2, target_client);

    let heartbeat = Helper::heartbeat(
        0,
        Some(0),
        vec![
            Helper::session_write(1, 0, "foo", "bar", session1.clone()),
            Helper::session_write(2, 0, "foo2", "bar", session2.clone()),
            Helper::session_write(3, 0, "foo2", "bar", session3.clone()),
        ],
    );

    // WHEN
    cluster_actor.replicate(heartbeat).await;

    // THEN
    assert_eq!(cluster_actor.replication.logger.last_log_index, 3);
    assert_eq!(cluster_actor.client_sessions.len(), 2);
}

#[tokio::test]
async fn follower_cluster_actor_replicate_state_only_upto_con_idx() {
    // GIVEN
    let mut follower_c_actor = Helper::cluster_actor(ReplicationRole::Follower).await;

    // Log two entries but don't commit them yet
    let heartbeat = Helper::heartbeat(
        0,
        Some(0), // con_idx=0, nothing committed yet
        vec![Helper::write(1, 0, "foo", "bar"), Helper::write(2, 0, "foo2", "bar")],
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };
    follower_c_actor.cache_manager = cache_manager.clone();

    // This just appends the entries to the log but doesn't commit them
    follower_c_actor.replicate(heartbeat).await;

    // WHEN - commit only up to index 1
    let task = tokio::spawn(async move {
        let mut received_foo = false;

        while let Some(message) = rx.recv().await {
            match message {
                | CacheCommand::Set { cache_entry } => {
                    let (key, value) = cache_entry.destructure();
                    if key == "foo" {
                        received_foo = true;
                        assert_eq!(value, "bar");
                    } else if key == "foo2" {
                        // This should not happen in our test
                        panic!("foo2 should not be applied yet");
                    }
                },
                | _ => continue,
            }
        }

        received_foo
    });

    // Send a heartbeat with con_idx=1 to commit only the first entry
    const CON_IDX: u64 = 1;
    let heartbeat = Helper::heartbeat(0, CON_IDX.into(), vec![]);
    follower_c_actor.replicate(heartbeat).await;

    // THEN
    // Give the task a chance to process the message
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel the task since we're done checking
    task.abort();

    // Verify state
    assert_eq!(follower_c_actor.replication.logger.con_idx.load(Ordering::Relaxed), 1);
    assert_eq!(follower_c_actor.replication.logger.last_log_index, 2);
}

#[tokio::test]
async fn test_apply_multiple_committed_entries() {
    // GIVEN
    let mut follower_c_actor = Helper::cluster_actor(ReplicationRole::Follower).await;

    // Add multiple entries
    let entries = vec![
        Helper::write(1, 0, "key1", "value1"),
        Helper::write(2, 0, "key2", "value2"),
        Helper::write(3, 0, "key3", "value3"),
    ];

    let heartbeat = Helper::heartbeat(1, 0.into(), entries);

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

    // First append entries but don't commit
    follower_c_actor.cache_manager = cache_manager.clone();
    follower_c_actor.replicate(heartbeat).await;

    // Create a task to monitor applied entries
    let monitor_task = tokio::spawn(async move {
        let mut applied_keys = Vec::new();

        while let Some(message) = rx.recv().await {
            if let CacheCommand::Set { cache_entry } = message {
                applied_keys.push(cache_entry.key().to_string());
                if applied_keys.len() == 3 {
                    break;
                }
            }
        }

        applied_keys
    });

    follower_c_actor.cache_manager = cache_manager.clone();

    // WHEN - commit all entries
    let commit_heartbeat = Helper::heartbeat(1, Some(3), vec![]);
    follower_c_actor.replicate(commit_heartbeat).await;

    // THEN
    // Verify that all entries were committed and applied in order
    let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
        .await
        .expect("Timeout waiting for entries to be applied")
        .expect("Task failed");

    assert_eq!(applied_keys, vec!["key1", "key2", "key3"]);
    assert_eq!(follower_c_actor.replication.logger.con_idx.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_partial_commit_with_new_entries() {
    // GIVEN
    let mut follower_c_actor = Helper::cluster_actor(ReplicationRole::Follower).await;

    // First, append some entries
    let first_entries =
        vec![Helper::write(1, 0, "key1", "value1"), Helper::write(2, 0, "key2", "value2")];

    let first_heartbeat = Helper::heartbeat(1, Some(0), first_entries);

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

    follower_c_actor.cache_manager = cache_manager.clone();
    follower_c_actor.replicate(first_heartbeat).await;

    // Create a task to monitor applied entries
    let monitor_task = tokio::spawn(async move {
        let mut applied_keys = Vec::new();

        while let Some(message) = rx.recv().await {
            if let CacheCommand::Set { cache_entry } = message {
                applied_keys.push(cache_entry.key().to_string());
                if applied_keys.len() == 1 {
                    break;
                }
            }
        }

        applied_keys
    });

    // WHEN - commit partial entries and append new ones
    let second_entries = vec![Helper::write(3, 0, "key3", "value3")];

    let second_heartbeat = Helper::heartbeat(1, Some(1), second_entries);

    follower_c_actor.replicate(second_heartbeat).await;

    // THEN
    // Verify that only key1 was applied
    let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
        .await
        .expect("Timeout waiting for entries to be applied")
        .expect("Task failed");

    assert_eq!(applied_keys, vec!["key1"]);
    assert_eq!(follower_c_actor.replication.logger.con_idx.load(Ordering::Relaxed), 1);
    assert_eq!(follower_c_actor.replication.logger.last_log_index, 3); // All entries are in the log
}

#[tokio::test]
async fn follower_truncates_log_on_term_mismatch() {
    // GIVEN: A follower with an existing log entry at index  term 1
    let mut inmemory = MemoryOpLogs::default();
    //prefill

    inmemory
        .writer
        .extend(vec![Helper::write(2, 1, "key1", "val1"), Helper::write(3, 1, "key2", "val2")]);

    let logger = ReplicatedLogs::new(inmemory, 3, 1);

    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    cluster_actor.replication.logger = logger;

    // Simulate an initial log entry at index 1, term 1
    // WHEN: Leader sends an AppendEntries with prev_log_index=1, prev_log_term=2 (mismatch)
    let mut heartbeat = Helper::heartbeat(2, Some(0), vec![Helper::write(4, 0, "key2", "val2")]);
    heartbeat.prev_log_term = 0;
    heartbeat.prev_log_index = 2;

    let result = cluster_actor.replicate_log_entries(&mut heartbeat).await;

    // THEN: Expect truncation and rejection
    assert_eq!(cluster_actor.replication.logger.target.writer.len(), 1);
    assert!(result.is_err(), "Should reject due to term mismatch");
}

#[tokio::test]
async fn follower_accepts_entries_with_empty_log_and_prev_log_index_zero() {
    // GIVEN: A follower with an empty log

    let mut follower_c_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // WHEN: Leader sends entries with prev_log_index=0
    let mut heartbeat = Helper::heartbeat(1, Some(0), vec![Helper::write(1, 0, "key1", "val1")]);

    let result = follower_c_actor.replicate_log_entries(&mut heartbeat).await;

    // THEN: Entries are accepted
    assert!(result.is_ok(), "Should accept entries with prev_log_index=0 on empty log");
    assert_eq!(follower_c_actor.replication.logger.last_log_index, 1); // Assuming write_log_entries updates log_index
}

#[tokio::test]
async fn follower_rejects_entries_with_empty_log_and_prev_log_index_nonzero() {
    // GIVEN: A follower with an empty log

    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // WHEN: Leader sends entries with prev_log_index=1
    let mut heartbeat = Helper::heartbeat(1, Some(0), vec![Helper::write(2, 0, "key2", "val2")]);
    heartbeat.prev_log_index = 1;
    heartbeat.prev_log_term = 1;

    let result = cluster_actor.replicate_log_entries(&mut heartbeat).await;

    // THEN: Entries are rejected
    assert!(result.is_err(), "Should reject entries with prev_log_index > 0 on empty log");
    assert_eq!(cluster_actor.replication.logger.last_log_index, 0); // Log should remain unchanged
}

#[tokio::test]
async fn req_consensus_inserts_consensus_voting() {
    // GIVEN

    let mut leader_c_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let replid = leader_c_actor.replication.replid.clone();
    // - add 5 followers
    let (cluster_sender, _) = ClusterActorQueue::new(100);

    let follower_buffs = (0..5).map(|_| FakeReadWrite::new()).collect::<Vec<_>>();
    Helper::cluster_member(
        &mut leader_c_actor,
        follower_buffs.clone(),
        cluster_sender,
        0,
        Some(replid),
    );

    let (callback, _) = Callback::create();
    let client_id = Uuid::now_v7().to_string();
    let session_request = SessionRequest::new(1, client_id);
    let w_req = LogEntry::Set { key: "foo".into(), value: "bar".into(), expires_at: None };
    let consensus_request =
        ConsensusRequest::new(w_req.clone(), callback, Some(session_request.clone()));

    // WHEN
    leader_c_actor.req_consensus(consensus_request).await;

    // THEN
    assert_eq!(leader_c_actor.consensus_tracker.len(), 1);
    assert_eq!(leader_c_actor.replication.logger.last_log_index, 1);

    assert_eq!(
        leader_c_actor.consensus_tracker.get(&1).unwrap().session_req.as_ref().unwrap().clone(), //* session_request_is_saved_on_tracker
        session_request
    );

    // check on follower_buffs
    for follower in follower_buffs {
        assert_expected_queryio(
            &follower,
            QueryIO::AppendEntriesRPC(HeartBeat {
                from: leader_c_actor.replication.self_identifier(),
                replid: leader_c_actor.replication.replid.clone(),
                append_entries: vec![WriteOperation {
                    request: w_req.clone(),
                    log_index: 1,
                    term: 0,
                    session_req: Some(session_request.clone()),
                }],
                leader_commit_idx: Some(0),
                ..Default::default()
            }),
        )
        .await;
    }
}

#[tokio::test]
async fn test_leader_req_consensus_early_return_when_already_processed_session_req_given() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let client_id = Uuid::now_v7().to_string();
    let client_req = SessionRequest::new(1, client_id);

    // WHEN - session request is already processed
    cluster_actor.client_sessions.set_response(Some(client_req.clone()));
    let handler = cluster_actor.self_handler.clone();
    tokio::spawn(cluster_actor.handle());
    let (tx, rx) = Callback::create();
    handler
        .send(ClusterCommand::Client(ClientMessage::LeaderReqConsensus(ConsensusRequest::new(
            LogEntry::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            tx,
            Some(client_req),
        ))))
        .await
        .unwrap();

    // THEN
    rx.recv().await;
}

#[tokio::test]
async fn test_consensus_voting_deleted_when_consensus_reached() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };
    cluster_actor.cache_manager = cache_manager.clone();

    let replid = cluster_actor.replication.replid.clone();
    let (cluster_sender, _) = ClusterActorQueue::new(100);

    // - add 4 followers to create quorum - so 2 votes are needed to reach consensus
    let follower_buffs = (0..4).map(|_| FakeReadWrite::new()).collect::<Vec<_>>();
    Helper::cluster_member(
        &mut cluster_actor,
        follower_buffs.clone(),
        cluster_sender,
        0,
        Some(replid),
    );
    let (client_request_sender, client_wait) = Callback::create();

    let client_id = Uuid::now_v7().to_string();
    let client_request = SessionRequest::new(3, client_id);
    let consensus_request =
        Helper::consensus_request(client_request_sender, Some(client_request.clone()));

    cluster_actor.req_consensus(consensus_request).await;

    // WHEN
    let follower_res = ReplicationAck { log_idx: 1, term: 0, rej_reason: None };
    let (first_repl, _) = cluster_actor.replicas().next().unwrap();
    // Leader already has 1 vote, so we only need 1 more votes to reach consensus
    cluster_actor.ack_replication(first_repl.clone(), follower_res.clone()).await;

    // up to this point, tracker hold the consensus
    assert_eq!(cluster_actor.consensus_tracker.len(), 1);
    assert_eq!(cluster_actor.consensus_tracker.get(&1).unwrap().voters.len(), 1);

    // ! Majority votes made
    let (last_repl, _) = cluster_actor.members.iter().last().unwrap();
    cluster_actor.ack_replication(last_repl.clone(), follower_res).await;

    // THEN
    assert_eq!(cluster_actor.consensus_tracker.len(), 0);
    assert_eq!(cluster_actor.replication.logger.last_log_index, 1);

    client_wait.recv().await;
    assert!(cluster_actor.client_sessions.is_processed(&Some(client_request))); // * session_request_is_marked_as_processed
}

#[tokio::test]
async fn test_same_voter_can_vote_only_once() {
    // GIVEN
    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    cluster_actor.cache_manager = cache_manager.clone();

    let replid = cluster_actor.replication.replid.clone();
    let (cluster_sender, _) = ClusterActorQueue::new(100);

    // - add followers to create quorum

    let follower_buffs = (0..4).map(|_| FakeReadWrite::new()).collect::<Vec<_>>();
    Helper::cluster_member(
        &mut cluster_actor,
        follower_buffs.clone(),
        cluster_sender,
        0,
        Some(replid),
    );
    let (client_request_sender, _client_wait) = Callback::create();

    let consensus_request = Helper::consensus_request(client_request_sender, None);

    cluster_actor.req_consensus(consensus_request).await;

    // WHEN
    assert_eq!(cluster_actor.consensus_tracker.len(), 1);
    let follower_res = ReplicationAck { log_idx: 1, term: 0, rej_reason: None };
    let from = PeerIdentifier("repl1".to_string());
    cluster_actor.ack_replication(from.clone(), follower_res.clone()).await;
    cluster_actor.ack_replication(from.clone(), follower_res.clone()).await;
    cluster_actor.ack_replication(from, follower_res).await;

    // THEN - no change in consensus tracker even though the same voter voted multiple times
    assert_eq!(cluster_actor.consensus_tracker.len(), 1);
    assert_eq!(cluster_actor.replication.logger.last_log_index, 1);
}
#[tokio::test]
async fn leader_consensus_tracker_not_changed_when_followers_not_exist() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let (tx, _rx) = tokio::sync::mpsc::channel(100);
    let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };
    cluster_actor.cache_manager = cache_manager.clone();
    let (tx, _rx) = Callback::create();

    let consensus_request = Helper::consensus_request(tx, None);

    // WHEN
    cluster_actor.req_consensus(consensus_request).await;

    // THEN
    assert_eq!(cluster_actor.consensus_tracker.len(), 0);
    assert_eq!(cluster_actor.replication.logger.last_log_index, 1);
}

#[tokio::test]
async fn test_leader_req_consensus_with_pending_requests() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // Block write requests to create pending requests queue
    cluster_actor.block_write_reqs();

    assert_eq!(cluster_actor.migrations_in_progress.as_ref().unwrap().num_reqs(), 0);
    assert_eq!(cluster_actor.migrations_in_progress.as_ref().unwrap().num_batches(), 0);

    let (tx, _) = Callback::create();
    let consensus_request = Helper::consensus_request(tx, None);

    // WHEN - send request while write requests are blocked
    cluster_actor.leader_req_consensus(consensus_request).await;

    // THEN
    assert!(cluster_actor.migrations_in_progress.is_some());
    assert_eq!(cluster_actor.migrations_in_progress.as_ref().unwrap().num_reqs(), 1);
    assert_eq!(cluster_actor.replication.logger.last_log_index, 0);
}

#[tokio::test]
async fn test_leader_req_consensus_with_processed_session() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let client_id = Uuid::now_v7().to_string();
    let session_req = SessionRequest::new(1, client_id);

    // Mark the session as already processed
    cluster_actor.client_sessions.set_response(Some(session_req.clone()));

    // WHEN - send request with already processed session
    let (tx, rx) = Callback::create();
    let consensus_request = ConsensusRequest::new(
        LogEntry::Set { key: "test_key".into(), value: "test_value".into(), expires_at: None },
        tx,
        Some(session_req),
    );

    cluster_actor.leader_req_consensus(consensus_request).await;

    // THEN
    // Verify the request was not processed (no new log entry)
    assert_eq!(cluster_actor.replication.logger.last_log_index, 0);

    // Verify the response indicates already processed
    let ConsensusClientResponse::AlreadyProcessed { key } = rx.recv().await else {
        panic!("Expected AlreadyProcessed response");
    };
    assert_eq!(key, vec!["test_key".to_string()]);
}
