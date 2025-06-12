use crate::domains::QueryIO;
use crate::domains::cluster_actors::hash_ring::BatchId;
use crate::domains::cluster_actors::hash_ring::{
    HashRing, MigrationTask, tests::migration_task_create_helper,
};
use std::collections::HashMap;
use std::time::Duration;

use super::*;

// ! When LazyOption is Lazy, rebalance request should not block
#[tokio::test]
async fn test_rebalance_request_with_lazy() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Lazy;
    cluster_actor.rebalance_request(request_to, lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none())
}

// ! when member has not been connected, ignore
#[tokio::test]
async fn test_rebalance_request_before_member_connected() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to, lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none())
}

// ! rebalance request to replica should be ignored
#[tokio::test]
async fn test_rebalance_request_to_replica() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    let (buf, _) = cluster_actor.test_add_peer(6559, NodeKind::Replica, None);

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to.clone(), lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none());

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
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    let (buf, _) = cluster_actor.test_add_peer(
        6559,
        NodeKind::NonData,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
    );

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to.clone(), lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_some());
    assert_expected_queryio(&buf, QueryIO::StartRebalance).await;
}

#[tokio::test]
async fn test_start_rebalance_before_connection_is_made() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();

    // WHEN
    cluster_actor
        .start_rebalance(PeerIdentifier("127.0.0.1:6559".into()), &cache_manager, None)
        .await;

    // THEN
    // No pending requests should be created since the member is not connected
    assert!(cluster_actor.pending_requests.is_none());
}

// ! Failcase
#[tokio::test]
async fn test_start_rebalance_to_replica() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();
    let (buf, peer_id) = cluster_actor.test_add_peer(6559, NodeKind::Replica, None);

    // WHEN
    cluster_actor.start_rebalance(peer_id, &cache_manager, None).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none());
    let msg = buf.lock().await.pop_front();
    assert!(msg.is_none());
}

#[tokio::test]
async fn test_start_rebalance_happy_path() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();
    let (buf, peer_id) = cluster_actor.test_add_peer(
        6559,
        NodeKind::NonData,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
    );

    // WHEN
    cluster_actor.start_rebalance(peer_id, &cache_manager, None).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_some());

    assert_expected_queryio(
        &buf,
        QueryIO::ClusterHeartBeat(HeartBeat {
            from: cluster_actor.replication.self_identifier(),
            hashring: Some(cluster_actor.hash_ring.clone()),
            replid: cluster_actor.replication.replid.clone(),
            ..Default::default()
        }),
    )
    .await;
}

#[tokio::test]
async fn test_schedule_migration_if_required_when_noplan_is_made() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;
    tokio::time::sleep(Duration::from_millis(1)).await; // sleep to make sure last_modified is updated

    // Create hash ring for coordinating node
    let hash_ring = HashRing::default();
    let coordinator_replid = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let hash_ring = hash_ring
        .add_partition_if_not_exists(coordinator_replid, PeerIdentifier::new("127.0.0.1", 5999))
        .unwrap();
    let hash_ring = hash_ring
        .add_partition_if_not_exists(
            cluster_actor.replication.replid.clone(),
            cluster_actor.replication.self_identifier(),
        )
        .unwrap();

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    cluster_actor
        .schedule_migration_if_required(Some(hash_ring.clone()), &cache_manager, None)
        .await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none());
    assert_eq!(cluster_actor.hash_ring, hash_ring);
    assert_ne!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_given_hashring_is_same() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    cluster_actor
        .schedule_migration_if_required(Some(cluster_actor.hash_ring.clone()), &cache_manager, None)
        .await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_no_hashring_given() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    cluster_actor.schedule_migration_if_required(None, &cache_manager, None).await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_last_modified_is_lower_than_its_own() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = cluster_actor.hash_ring.last_modified;

    let mut hash_ring = HashRing::default();
    hash_ring.last_modified = last_modified - 1;

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    cluster_actor.schedule_migration_if_required(Some(hash_ring), &cache_manager, None).await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_send_migrate_and_wait_happypath() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    // Create dummy task
    let target_replid = ReplicationId::Key("my_test_key".to_string());
    let batch_to_migrate = vec![migration_task_create_helper(0, 100)];

    // WHEN
    tokio::spawn({
        async move {
            while let Some(msg) = rx.recv().await {
                // ! First check, message must be Scheduler Message
                let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationBatch(
                    batch,
                    callback,
                )) = msg
                else {
                    panic!()
                };
                assert_eq!(batch.target_repl, ReplicationId::Key("my_test_key".to_string()));
                let _ = callback.send(Ok(()));
            }
        }
    });

    let result = ClusterActor::<MemoryOpLogs>::schedule_migration_in_batch(
        target_replid,
        batch_to_migrate,
        fake_handler,
    )
    .await;

    // THEN
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_send_migrate_and_wait_channel_error() {
    // GIVEN
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    let target_replid = ReplicationId::Key("error_test".to_string());
    let batch_to_migrate = vec![migration_task_create_helper(0, 10)];

    // WHEN - drop receiver to simulate channel closure
    drop(rx);

    let result = ClusterActor::<MemoryOpLogs>::schedule_migration_in_batch(
        target_replid,
        batch_to_migrate,
        fake_handler,
    )
    .await;

    // THEN - should handle gracefully with error
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "channel closed");
}

#[tokio::test]
async fn test_send_migrate_and_wait_callback_error() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    let target_replid = ReplicationId::Key("error_response_test".to_string());
    let batch_to_migrate = vec![migration_task_create_helper(0, 10)];

    // WHEN - simulate error response from migration handler
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationBatch(_, callback)) =
                msg
            else {
                panic!("Expected MigrateBatchKeys message");
            };

            // Send error response
            let _ = callback.send(Err(anyhow::anyhow!("Simulated migration error")));
        }
    });

    let result = ClusterActor::<MemoryOpLogs>::schedule_migration_in_batch(
        target_replid,
        batch_to_migrate,
        fake_handler,
    )
    .await;

    // THEN - should return the error
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Simulated migration error"));
}

// Tests for migrate_keys function

#[tokio::test]
async fn test_migrate_keys_target_peer_not_found() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();

    let tasks = MigrationBatch::new(
        ReplicationId::Key("non_existent_peer".to_string()),
        vec![migration_task_create_helper(0, 5)],
    );
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

    // WHEN
    cluster_actor.migrate_batch(tasks, &cache_manager, callback_tx).await;

    // THEN
    let result = callback_rx.await.unwrap();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Target peer not found"));
}

#[tokio::test]
async fn test_migrate_keys_retrieves_actual_data() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    cluster_actor.block_write_reqs();

    let (_hwm, cache_manager) = cache_manager_create_helper();
    let target_repl_id = ReplicationId::Key("data_target".to_string());
    let (_, _) = cluster_actor.test_add_peer(6564, NodeKind::NonData, Some(target_repl_id.clone()));

    // Set up test data in cache
    cache_manager
        .route_set("test_key_1".to_string(), "value_1".to_string(), None, 1)
        .await
        .unwrap();
    cache_manager
        .route_set("test_key_2".to_string(), "value_2".to_string(), None, 2)
        .await
        .unwrap();

    let migration_task = MigrationTask {
        task_id: (0, 100),
        keys_to_migrate: vec!["test_key_1".to_string(), "test_key_2".to_string()],
    };
    let tasks = MigrationBatch::new(target_repl_id, vec![migration_task]);
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

    // WHEN
    cluster_actor.migrate_batch(tasks, &cache_manager, callback_tx).await;

    // THEN
    let pending_migrations = cluster_actor.pending_migrations.unwrap();
    assert_eq!(pending_migrations.len(), 1);

    // Verify data is still in cache (migration doesn't remove it yet)
    let retrieved_value_1 = cache_manager.route_get("test_key_1").await.unwrap();
    let retrieved_value_2 = cache_manager.route_get("test_key_2").await.unwrap();
    assert!(retrieved_value_1.is_some());
    assert!(retrieved_value_2.is_some());
    assert_eq!(retrieved_value_1.unwrap().value(), "value_1");
    assert_eq!(retrieved_value_2.unwrap().value(), "value_2");

    // Callback should not be called since migration is not completed
    let result = tokio::time::timeout(Duration::from_millis(100), callback_rx).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_receive_batch_success_path_when_consensus_is_required() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();
    let current_index = cluster_actor.logger.last_log_index;
    let peer_replid = ReplicationId::Key("repl_id_for_other_node".to_string());

    let (repl_buf, _) = cluster_actor.test_add_peer(6579, NodeKind::Replica, None);

    let (_, sender_peer_id) =
        cluster_actor.test_add_peer(6567, NodeKind::NonData, Some(peer_replid.clone()));
    cluster_actor.hash_ring = cluster_actor
        .hash_ring
        .add_partition_if_not_exists(peer_replid.clone(), sender_peer_id.clone())
        .unwrap();

    let cache_entries = cache_entries_create_helper(&[("success_key3", "value2")]);
    let migrate_batch = migration_batch_create_helper("success_test", cache_entries.clone());

    // WHEN
    cluster_actor.receive_batch(migrate_batch, &cache_manager, sender_peer_id).await;

    // THEN - verify that the log index is incremented
    assert_eq!(cluster_actor.logger.last_log_index, current_index + 1);

    assert_expected_queryio(
        &repl_buf,
        QueryIO::AppendEntriesRPC(HeartBeat {
            from: cluster_actor.replication.self_identifier(),
            replid: cluster_actor.replication.replid.clone(),
            append_entries: vec![WriteOperation {
                request: WriteRequest::MSet { entries: cache_entries.clone() },
                log_index: 1,
                term: 0,
            }],
            ..Default::default()
        }),
    )
    .await;
}

#[tokio::test]
async fn test_receive_batch_success_path_when_noreplica_found() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();
    let current_index = cluster_actor.logger.last_log_index;
    let peer_replid = ReplicationId::Key("repl_id_for_other_node".to_string());

    let (_, sender_peer_id) =
        cluster_actor.test_add_peer(6567, NodeKind::NonData, Some(peer_replid.clone()));
    cluster_actor.hash_ring = cluster_actor
        .hash_ring
        .add_partition_if_not_exists(peer_replid.clone(), sender_peer_id.clone())
        .unwrap();

    let cache_entries =
        cache_entries_create_helper(&[("success_key3", "value2"), ("success_key4", "value4")]);
    let migrate_batch = migration_batch_create_helper("success_test", cache_entries.clone());

    // WHEN
    cluster_actor.receive_batch(migrate_batch, &cache_manager, sender_peer_id).await;

    // THEN - verify that the log index is incremented
    assert_eq!(cluster_actor.logger.last_log_index, current_index + 1);
    let keys = cache_manager.route_keys(None).await;
    assert_eq!(keys.len(), 2);
}

#[tokio::test]
async fn test_receive_batch_validation_failure_keys_not_belonging_to_node() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    // Create a hash ring where another node is responsible for the keys
    let hash_ring = HashRing::default();
    let other_node_replid = ReplicationId::Key("other_node".to_string());
    let hash_ring = hash_ring
        .add_partition_if_not_exists(other_node_replid, PeerIdentifier("127.0.0.1:5000".into()))
        .unwrap();
    cluster_actor.hash_ring = hash_ring;

    // ! this is the one for receiving actor

    let (_hwm, cache_manager) = cache_manager_create_helper();
    let (message_buf, sender_peer_id) = cluster_actor.test_add_peer(6565, NodeKind::NonData, None);

    let cache_entries = cache_entries_create_helper(&[("key1", "value1"), ("key2", "value2")]);
    let migrate_batch = migration_batch_create_helper("validation_test", cache_entries);

    // WHEN
    cluster_actor.receive_batch(migrate_batch, &cache_manager, sender_peer_id).await;

    // THEN
    assert_expected_queryio(
        &message_buf,
        QueryIO::MigrationBatchAck(MigrationBatchAck {
            batch_id: BatchId("validation_test".into()),
            success: false,
        }),
    )
    .await;
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_when_no_pending_migrations() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(2).await;
    cluster_actor.pending_migrations = Some(HashMap::new());

    // WHEN
    cluster_actor.unblock_write_reqs_if_done();

    // THEN
    assert!(cluster_actor.pending_requests.is_none());
    assert!(cluster_actor.pending_migrations.is_none());
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_when_migrations_still_pending() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;

    // Add pending migration (simulating migration still in progress)
    let (callback, _migration_rx) = tokio::sync::oneshot::channel();
    let batch_id = BatchId("test_batch".into());
    cluster_actor
        .pending_migrations
        .as_mut()
        .unwrap()
        .insert(batch_id, PendingMigrationBatch::new(callback, vec![]));

    // WHEN
    cluster_actor.unblock_write_reqs_if_done();

    // THEN - Nothing should change - requests should remain blocked
    assert!(cluster_actor.pending_requests.is_some());
    assert_eq!(cluster_actor.pending_requests.as_ref().unwrap().len(), 1);
    assert!(cluster_actor.pending_migrations.is_some());
    assert_eq!(cluster_actor.pending_migrations.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_when_not_blocked() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    cluster_actor.pending_migrations = Some(HashMap::new());

    // WHEN
    cluster_actor.unblock_write_reqs_if_done();

    // THEN - Should not crash and pending_migrations should remain as empty
    assert!(cluster_actor.pending_requests.is_none());
    assert!(cluster_actor.pending_migrations.is_some());
    assert!(cluster_actor.pending_migrations.as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn test_unblock_write_reqs_if_done_multiple_times() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;
    cluster_actor.pending_migrations = Some(HashMap::new());

    // WHEN - call unblock multiple times
    cluster_actor.unblock_write_reqs_if_done();
    cluster_actor.unblock_write_reqs_if_done();
    cluster_actor.unblock_write_reqs_if_done();

    // THEN - Should be idempotent
    assert!(cluster_actor.pending_requests.is_none());
    assert!(cluster_actor.pending_migrations.is_none());
}

#[tokio::test]
async fn test_find_target_peer_for_replication() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let repl_id_1 = ReplicationId::Key("repl_1".to_string());
    let repl_id_2 = ReplicationId::Key("repl_2".to_string());

    let (_, peer_id_1) =
        cluster_actor.test_add_peer(6561, NodeKind::NonData, Some(repl_id_1.clone()));
    let (_, peer_id_2) =
        cluster_actor.test_add_peer(6562, NodeKind::NonData, Some(repl_id_2.clone()));

    // WHEN & THEN
    assert_eq!(cluster_actor.peerid_by_replid(&repl_id_1), Some(&peer_id_1));
    assert_eq!(cluster_actor.peerid_by_replid(&repl_id_2), Some(&peer_id_2));
    assert_eq!(
        cluster_actor.peerid_by_replid(&ReplicationId::Key("non_existent".to_string())),
        None
    );
}

#[tokio::test]
async fn test_handle_migration_ack_failure() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;
    let (_hwm, _cache_manager) = cache_manager_create_helper();
    let (callback, callback_rx) = tokio::sync::oneshot::channel();
    let batch_id = BatchId("failure_batch".into());

    // Ensure pending_migrations is set up
    assert!(cluster_actor.pending_migrations.is_some());
    cluster_actor
        .pending_migrations
        .as_mut()
        .unwrap()
        .insert(batch_id.clone(), PendingMigrationBatch::new(callback, vec![]));

    let ack = MigrationBatchAck::with_reject(batch_id.clone());

    // WHEN
    let result = cluster_actor.handle_migration_ack(ack, &_cache_manager).await;

    // THEN
    assert!(result.is_some()); // Method should return Some(())

    // Verify callback was called with error
    let callback_result = callback_rx.await.unwrap();
    assert!(callback_result.is_err());
    assert!(
        callback_result
            .unwrap_err()
            .to_string()
            .starts_with("Failed to send migration completion signal for batch")
    );
}

#[tokio::test]
async fn test_handle_migration_ack_batch_id_not_found() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;
    let (_hwm, _cache_manager) = cache_manager_create_helper();
    let (callback, _callback_rx) = tokio::sync::oneshot::channel();
    let existing_batch_id = BatchId("existing_batch".into());
    cluster_actor
        .pending_migrations
        .as_mut()
        .unwrap()
        .insert(existing_batch_id, PendingMigrationBatch::new(callback, vec![]));

    let non_existent_batch_id = BatchId("non_existent_batch".into());
    let ack = MigrationBatchAck { batch_id: non_existent_batch_id, success: true };

    // WHEN
    let result = cluster_actor.handle_migration_ack(ack, &_cache_manager).await;

    // THEN
    assert!(result.is_none()); // Method should return None when batch ID is not found

    // Verify existing batch is still there
    assert_eq!(cluster_actor.pending_migrations.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn test_handle_migration_ack_success_case_with_pending_reqs_and_migration() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(2).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();

    // Set up test keys in cache that will be part of the migration
    let test_keys = vec!["migrate_key_1".to_string(), "migrate_key_2".to_string()];
    cache_manager
        .route_set("migrate_key_1".to_string(), "value_1".to_string(), None, 1)
        .await
        .unwrap();
    cache_manager
        .route_set("migrate_key_2".to_string(), "value_2".to_string(), None, 2)
        .await
        .unwrap();

    // Verify keys exist before migration
    assert!(cache_manager.route_get("migrate_key_1").await.unwrap().is_some());
    assert!(cache_manager.route_get("migrate_key_2").await.unwrap().is_some());

    // Add the last pending migration with the test keys
    let (callback, callback_rx) = tokio::sync::oneshot::channel();
    let batch_id = BatchId("last_batch".into());
    cluster_actor
        .pending_migrations
        .as_mut()
        .unwrap()
        .insert(batch_id.clone(), PendingMigrationBatch::new(callback, test_keys));

    let ack = MigrationBatchAck { batch_id, success: true };

    // Verify initially blocked
    assert!(cluster_actor.pending_requests.is_some());
    assert_eq!(cluster_actor.pending_requests.as_ref().unwrap().len(), 2);
    assert!(cluster_actor.pending_migrations.is_some());
    assert_eq!(cluster_actor.pending_migrations.as_ref().unwrap().len(), 1);

    // WHEN
    let result = cluster_actor.handle_migration_ack(ack, &cache_manager).await;

    // THEN
    assert!(result.is_some());

    // Verify callback was successful
    let callback_result = callback_rx.await.unwrap();
    assert!(callback_result.is_ok());

    // Verify keys were deleted from cache after successful migration
    assert!(cache_manager.route_get("migrate_key_1").await.unwrap().is_none());
    assert!(cache_manager.route_get("migrate_key_2").await.unwrap().is_none());

    // TODO - do it after making sender and receiver test double.
    // Verify unblock_write_reqs_if_done was called and requests were unblocked
    // Since this was the last migration, both should be None now
    // assert!(cluster_actor.pending_requests.is_none());
    // assert!(cluster_actor.pending_migrations.is_none());
}

// Verify that the start_rebalance -> schedule_migration_if_required flow works.
// This test addresses the TODO comment: "need to see if migration batch is scheduled."
#[tokio::test]
async fn test_start_rebalance_schedules_migration_batches() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper_with_keys(vec![
        "test_key_1".to_string(),
        "test_key_2".to_string(),
    ])
    .await;

    // ! test_key_1 and test_key_2 are migrated to testnode_a
    let target_repl_id = ReplicationId::Key("testnode_a".into());
    let (buf, peer_id) = cluster_actor.test_add_peer(
        6570,
        NodeKind::NonData,
        Some(ReplicationId::Key("testnode_a".into())),
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let cluster_handler = ClusterCommandHandler(tx);

    // WHEN
    cluster_actor.start_rebalance(peer_id.clone(), &cache_manager, Some(cluster_handler)).await;

    // THEN
    // 1. Verify heartbeat was sent immediately (synchronous part)
    assert_expected_queryio(
        &buf,
        QueryIO::ClusterHeartBeat(HeartBeat {
            from: cluster_actor.replication.self_identifier(),
            hashring: Some(cluster_actor.hash_ring.clone()),
            replid: cluster_actor.replication.replid.clone(),
            ..Default::default()
        }),
    )
    .await;

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

    assert_eq!(batch.target_repl, target_repl_id);
    assert!(!batch.tasks.is_empty());

    // 3. Verify pending_requests is set (synchronous part)
    assert!(cluster_actor.pending_requests.is_some());
}
