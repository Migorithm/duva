use super::*;
use crate::domains::QueryIO;
use crate::domains::caches::cache_objects::{CacheValue, TypedValue};
use crate::domains::cluster_actors::hash_ring::{HashRing, tests::migration_task_create_helper};
use std::time::Duration;

// ! When LazyOption is Lazy, rebalance request should not block
#[tokio::test]
async fn test_rebalance_request_with_lazy() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Lazy;
    cluster_actor.rebalance_request(request_to, lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_reqs.is_none())
}

// ! when member has not been connected, ignore
#[tokio::test]
async fn test_rebalance_request_before_member_connected() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to, lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_reqs.is_none())
}

// ! rebalance request to replica should be ignored
#[tokio::test]
async fn test_rebalance_request_to_replica() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let (buf, _) = cluster_actor.test_add_peer(6559, None, false);

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to.clone(), lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_reqs.is_none());

    let msg = buf.lock().await.pop_front();
    assert!(msg.is_none());
}

// * happy path
// - NonData Peer
// - Eager LazyOption
// - member connected
#[tokio::test]
async fn test_rebalance_request_happypath() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let (buf, _) = cluster_actor.test_add_peer(
        6559,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
        true,
    );

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to.clone(), lazy_o).await;

    // THEN
    // At this point, the re-balance request should not block the requests
    // requests will be blocked only if migrations needed.
    assert!(cluster_actor.pending_reqs.is_none());
    assert_expected_queryio(&buf, QueryIO::StartRebalance).await;
}

#[tokio::test]
async fn test_start_rebalance_before_connection_is_made() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // WHEN
    let _ = cluster_actor.start_rebalance().await;

    // THEN
    // No pending requests should be created since the member is not connected
    assert!(cluster_actor.pending_reqs.is_none());
}

// ! Failcase
#[tokio::test]
async fn test_start_rebalance_only_when_replica_is_found() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let (buf, _) = cluster_actor.test_add_peer(6559, None, false);

    // WHEN
    let _ = cluster_actor.start_rebalance().await;

    // THEN
    assert!(cluster_actor.pending_reqs.is_none());
    let msg = buf.lock().await.pop_front();
    assert!(msg.is_none());
}

#[tokio::test]
async fn test_start_rebalance_happy_path() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let (buf, _) = cluster_actor.test_add_peer(
        6559,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
        true,
    );

    // WHEN

    cluster_actor.start_rebalance().await;

    // THEN
    assert_expected_queryio(
        &buf,
        QueryIO::ClusterHeartBeat(HeartBeat {
            from: cluster_actor.replication.self_identifier(),
            hashring: Some(Box::new(cluster_actor.hash_ring.clone())),
            replid: cluster_actor.replication.replid.clone(),
            leader_commit_idx: Some(0),
            ..Default::default()
        }),
    )
    .await;
}

// Verify that the start_rebalance -> maybe_update_hashring flow works.
#[tokio::test]
async fn test_start_rebalance_schedules_migration_batches() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let (_con_idx, cache_manager) =
        Helper::cache_manager_with_keys(vec!["test_key_1".to_string(), "test_key_2".to_string()])
            .await;

    // ! test_key_1 and test_key_2 are migrated to testnode_a
    let target_repl_id = ReplicationId::Key("testnode_a".into());
    let (buf, _leader_for_diff_shard) =
        cluster_actor.test_add_peer(6570, Some(ReplicationId::Key("testnode_a".into())), true);

    let (cluster_handler, mut rx) = ClusterActorQueue::create(2);
    cluster_actor.cache_manager = cache_manager.clone();

    // WHEN
    cluster_actor.self_handler = cluster_handler.clone();
    cluster_actor.start_rebalance().await;

    // THEN
    // 1. Verify heartbeat was sent immediately (synchronous part)
    let QueryIO::ClusterHeartBeat(HeartBeat { hashring, .. }) =
        buf.lock().await.pop_front().unwrap()
    else {
        panic!()
    };

    assert!(hashring.is_some());
    assert_ne!(*hashring.unwrap(), cluster_actor.hash_ring);

    // 2. Wait for migration batch message with timeout (asynchronous part)
    let batch = tokio::time::timeout(Duration::from_millis(1000), async {
        loop {
            match rx.recv().await {
                | Some(ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationBatch(
                    b,
                    _,
                ))) => return b,
                | Some(_) => continue, // Skip other message types
                | None => panic!("Channel closed without receiving ScheduleMigrationBatch"),
            }
        }
    })
    .await
    .expect("Should receive ScheduleMigrationBatch within timeout");

    assert_eq!(&batch.target_repl, &target_repl_id);
    assert!(!batch.chunks.is_empty());

    // 3. Verify pending_requests is set (synchronous part)
    assert!(cluster_actor.pending_reqs.is_some());
}

#[tokio::test]
async fn test_maybe_update_hashring_when_noplan_is_made() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;
    tokio::time::sleep(Duration::from_millis(1)).await; // sleep to make sure last_modified is updated

    // Create hash ring for coordinating node
    let hash_ring = HashRing::default();
    let coordinator_replid = ReplicationId::Key(uuid::Uuid::now_v7().to_string());

    let hash_ring = hash_ring
        .set_partitions(vec![
            (coordinator_replid, PeerIdentifier::new("127.0.0.1", 5999)),
            (cluster_actor.replication.replid.clone(), cluster_actor.replication.self_identifier()),
        ])
        .unwrap();

    // WHEN
    cluster_actor.maybe_update_hashring(Some(Box::new(hash_ring.clone()))).await;

    // THEN
    assert!(cluster_actor.pending_reqs.is_none());
    assert_eq!(cluster_actor.hash_ring, hash_ring);
    assert_ne!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_given_hashring_is_same() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;

    // WHEN

    cluster_actor.maybe_update_hashring(Some(Box::new(cluster_actor.hash_ring.clone()))).await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_no_hashring_given() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;

    // WHEN
    cluster_actor.maybe_update_hashring(None).await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_send_migrate_and_wait_happypath() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    // Create dummy task
    let target_replid = ReplicationId::Key("my_test_key".to_string());
    let batch_to_migrate = vec![migration_task_create_helper(0, 100)];
    let batch = PendingMigrationTask::new(target_replid.clone(), batch_to_migrate.clone());

    // ! spawn actor receiver in the background
    let task = tokio::spawn(async move {
        while let Some(ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationBatch(_, d))) =
            cluster_actor.receiver.recv().await
        {
            d.send(Ok(()));
            break;
        }
    });

    ClusterActor::<MemoryOpLogs>::schedule_migration_in_batch(
        batch,
        cluster_actor.self_handler.clone(),
    )
    .await
    .unwrap();

    // THEN
    task.await.unwrap();
}

#[tokio::test]
async fn test_send_migrate_and_wait_callback_error() {
    // GIVEN
    let (fake_handler, mut rx) = ClusterActorQueue::create(10);

    let target_replid = ReplicationId::Key("error_response_test".to_string());
    let batch_to_migrate = vec![migration_task_create_helper(0, 10)];
    let batch = PendingMigrationTask::new(target_replid.clone(), batch_to_migrate.clone());
    // WHEN - simulate error response from migration handler
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationBatch(_, callback)) =
                msg
            else {
                panic!("Expected MigrateBatchKeys message");
            };

            // Send error response
            callback.send(Err(anyhow::anyhow!("Simulated migration error")));
        }
    });

    let result =
        ClusterActor::<MemoryOpLogs>::schedule_migration_in_batch(batch, fake_handler).await;

    // THEN - should return the error
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Simulated migration error"));
}

// Tests for migrate_keys function

#[tokio::test]
async fn test_migrate_keys_target_peer_not_found() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let tasks = PendingMigrationTask::new(
        ReplicationId::Key("non_existent_peer".to_string()),
        vec![migration_task_create_helper(0, 5)],
    );
    let (callback_tx, callback_rx) = Callback::create();

    // WHEN
    cluster_actor.migrate_batch(tasks, callback_tx).await;

    // THEN
    let result = callback_rx.recv().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Target peer not found"));
}

#[tokio::test]
async fn test_migrate_batch_send_migrate_batch_peer_message() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let cache_manager = CacheManager { cache_actor: CacheCommandSender(channel(10).0) };

    cluster_actor.cache_manager = cache_manager.clone();

    let replid = ReplicationId::Key("wheatever".to_string());
    let (buf, _id) = cluster_actor.test_add_peer(6909, Some(replid.clone()), true);

    let batch = PendingMigrationTask::new(replid.clone(), vec![migration_task_create_helper(0, 5)]);
    let (tx, _rx) = Callback::create();
    // WHEN
    cluster_actor.migrate_batch(batch.clone(), tx).await;

    // THEN
    assert_expected_queryio(
        &buf,
        QueryIO::MigrateBatch(BatchEntries { batch_id: batch.batch_id, entries: vec![] }),
    )
    .await;
}

#[tokio::test]
async fn test_receive_batch_when_empty_cache_entries() {
    //GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let replid = ReplicationId::Key("wheatever".to_string());
    let (buf, _id) = cluster_actor.test_add_peer(6909, Some(replid.clone()), true);

    // WHEN
    let batch = BatchEntries { batch_id: "empty_test".into(), entries: vec![] };
    cluster_actor.receive_batch(batch.clone(), &_id).await;

    // THEN - verify that no log index is incremented
    assert_eq!(cluster_actor.replication.logger.last_log_index, 0);
    assert_expected_queryio(&buf, QueryIO::MigrationBatchAck(batch.batch_id)).await;
}

#[tokio::test]
async fn test_receive_batch_when_consensus_is_required() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;

    let current_index = cluster_actor.replication.logger.last_log_index;
    let ack_to = PeerIdentifier::new("127.0.0.1", 6567);

    // add replica
    let (repl_buf, _) = cluster_actor.test_add_peer(6579, None, false);

    let entries = vec![CacheEntry::new("success_key3", "value2")];

    let batch = BatchEntries { batch_id: "success_test".into(), entries: entries.clone() };

    // WHEN
    cluster_actor.receive_batch(batch, &ack_to).await;

    // THEN - verify that the log index is incremented
    assert_eq!(cluster_actor.replication.logger.last_log_index, current_index + 1);
    assert_expected_queryio(
        &repl_buf,
        QueryIO::AppendEntriesRPC(HeartBeat {
            from: cluster_actor.replication.self_identifier(),
            replid: cluster_actor.replication.replid.clone(),
            append_entries: vec![WriteOperation {
                entry: LogEntry::MSet { entries: entries.clone() },
                log_index: 1,
                term: 0,
                session_req: None,
            }],
            leader_commit_idx: Some(0),
            ..Default::default()
        }),
    )
    .await;
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_when_no_pending_migrations() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(2).await;
    cluster_actor.pending_reqs = Some(Default::default());

    // WHEN
    cluster_actor.unblock_write_on_migration_done();

    // THEN
    assert!(cluster_actor.pending_reqs.is_none());
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_when_migrations_still_pending() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;

    // Add pending migration (simulating migration still in progress)
    let (callback, _migration_rx) = Callback::create();
    let batch_id = "test_batch".into();
    cluster_actor
        .pending_reqs
        .as_mut()
        .unwrap()
        .store_batch(batch_id, QueuedKeysToMigrate { callback, keys: vec![] });

    // WHEN
    cluster_actor.unblock_write_on_migration_done();

    // THEN - Nothing should change - requests should remain blocked
    assert!(cluster_actor.pending_reqs.is_some());
    assert_eq!(cluster_actor.pending_reqs.as_ref().unwrap().num_reqs(), 1);
    assert_eq!(cluster_actor.pending_reqs.unwrap().num_batches(), 1);
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_when_not_blocked() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    cluster_actor.pending_reqs = Some(Default::default());

    // WHEN
    cluster_actor.unblock_write_on_migration_done();

    // THEN - Should not crash and pending_migrations should remain as empty

    assert!(cluster_actor.pending_reqs.is_none());
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_multiple_times() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;
    cluster_actor.pending_reqs = Some(Default::default());

    // WHEN - call unblock multiple times
    cluster_actor.unblock_write_on_migration_done();
    cluster_actor.unblock_write_on_migration_done();
    cluster_actor.unblock_write_on_migration_done();

    // THEN - Should be idempotent

    assert!(cluster_actor.pending_reqs.is_none());
}

#[tokio::test]
async fn test_find_target_peer_for_replication() {
    // GIVEN
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    let repl_id_1 = ReplicationId::Key("repl_1".to_string());
    let repl_id_2 = ReplicationId::Key("repl_2".to_string());

    let (_, peer_id_1) = cluster_actor.test_add_peer(6561, Some(repl_id_1.clone()), true);
    let (_, peer_id_2) = cluster_actor.test_add_peer(6562, Some(repl_id_2.clone()), true);

    // WHEN & THEN
    assert_eq!(cluster_actor.peerid_by_replid(&repl_id_1), Some(&peer_id_1));
    assert_eq!(cluster_actor.peerid_by_replid(&repl_id_2), Some(&peer_id_2));
    assert_eq!(
        cluster_actor.peerid_by_replid(&ReplicationId::Key("non_existent".to_string())),
        None
    );
}

// Batch ID may not be found when the migration was already completed
#[tokio::test]
async fn test_handle_migration_ack_batch_id_not_found() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;

    let (callback, _callback_rx) = Callback::create();

    cluster_actor
        .pending_reqs
        .as_mut()
        .unwrap()
        .store_batch("existing_batch".into(), QueuedKeysToMigrate { callback, keys: vec![] });

    let non_existent_batch_id = "non_existent_batch".into();

    // WHEN
    cluster_actor.handle_migration_ack(non_existent_batch_id).await;

    // THEN
    assert_eq!(cluster_actor.pending_reqs.as_ref().unwrap().num_batches(), 1); // Verify existing batch is still there
}

#[tokio::test]
async fn test_handle_migration_ack_success_case_with_pending_reqs_and_migration() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(2).await;
    cluster_actor.test_add_peer(
        6055,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
        true,
    );

    let (_con_idx, cache_manager) = Helper::cache_manager();

    // Set up test keys in cache that will be part of the migration
    let test_keys = vec!["migrate_key_1".to_string(), "migrate_key_2".to_string()];
    cache_manager.route_set(CacheEntry::new("migrate_key_1", "value_1"), 1).await.unwrap();
    cache_manager.route_set(CacheEntry::new("migrate_key_2", "value_2"), 2).await.unwrap();

    // Verify keys exist before migration

    assert!(matches!(
        cache_manager.route_get("migrate_key_1").await,
        Ok(CacheValue { value: TypedValue::String(_), .. })
    ));
    assert!(matches!(
        cache_manager.route_get("migrate_key_2").await,
        Ok(CacheValue { value: TypedValue::String(_), .. })
    ));

    // Add the last pending migration with the test keys
    let (callback, _) = Callback::create();
    let batch_id = BatchId("last_batch".to_string());
    cluster_actor.cache_manager = cache_manager.clone();
    cluster_actor
        .pending_reqs
        .as_mut()
        .unwrap()
        .store_batch(batch_id.clone(), QueuedKeysToMigrate { callback, keys: test_keys });

    // Verify initially blocked
    assert_eq!(cluster_actor.pending_reqs.as_ref().unwrap().num_reqs(), 2);
    assert_eq!(cluster_actor.pending_reqs.as_ref().unwrap().num_batches(), 1);

    // WHEN
    let pre_num_batches = cluster_actor.pending_reqs.as_ref().unwrap().num_batches();
    cluster_actor.handle_migration_ack(batch_id).await;

    // THEN
    // Verify keys were deleted from cache after successful migration
    assert!(matches!(
        cache_manager.route_get("migrate_key_1").await,
        Ok(CacheValue { value: TypedValue::Null, .. })
    ));
    assert!(matches!(
        cache_manager.route_get("migrate_key_2").await,
        Ok(CacheValue { value: TypedValue::Null, .. })
    ));
    assert_eq!(pre_num_batches - 1, cluster_actor.pending_reqs.as_ref().unwrap().num_batches());
}

#[tokio::test]
async fn test_maybe_update_hashring_replica_only_updates_ring() {
    // GIVEN - Create a replica actor (not leader)
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Follower).await;
    let (_con_idx, cache_manager) = Helper::cache_manager_with_keys(vec![
        "replica_key_1".to_string(),
        "replica_key_2".to_string(),
    ])
    .await;

    let original_ring = cluster_actor.hash_ring.clone();
    let original_modified = original_ring.last_modified;

    // Create a new hash ring with different configuration
    let new_node_replid = ReplicationId::Key("new_node".to_string());
    let new_ring = HashRing::default()
        .set_partitions(vec![
            (new_node_replid, PeerIdentifier::new("127.0.0.1", 6000)),
            (cluster_actor.replication.replid.clone(), cluster_actor.replication.self_identifier()),
        ])
        .unwrap();
    cluster_actor.cache_manager = cache_manager.clone();
    // WHEN - Replica receives hash ring update
    cluster_actor.maybe_update_hashring(Some(Box::new(new_ring.clone()))).await;

    // THEN - Hash ring should be updated
    assert_eq!(cluster_actor.hash_ring, new_ring);
    assert_ne!(cluster_actor.hash_ring.last_modified, original_modified);

    // But no migration tasks should be initiated (no pending requests/migrations)
    assert!(cluster_actor.pending_reqs.is_none());
}
