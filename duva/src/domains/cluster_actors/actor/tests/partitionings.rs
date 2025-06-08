use crate::domains::QueryIO;
use crate::domains::cluster_actors::hash_ring::BatchId;
use crate::domains::cluster_actors::hash_ring::{
    HashRing, MigrationTask, tests::migration_task_create_helper,
};
use crate::domains::peers::PeerMessage;
use crate::domains::peers::command::PeerCommand;
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

    let msg = buf.lock().await.pop_front();
    assert!(msg.is_some());
    assert_eq!(msg.unwrap(), QueryIO::StartRebalance);
}

#[tokio::test]
async fn test_start_rebalance_before_connection_is_made() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();

    // WHEN
    cluster_actor.start_rebalance(PeerIdentifier("127.0.0.1:6559".into()), &cache_manager).await;

    // THEN
    // No pending requests should be created since the member is not connected
    assert!(cluster_actor.pending_requests.is_none());
    // No message should be sent to the peer
}

// ! Failcase
#[tokio::test]
async fn test_start_rebalance_to_replica() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();
    let (buf, peer_id) = cluster_actor.test_add_peer(6559, NodeKind::Replica, None);

    // WHEN
    cluster_actor.start_rebalance(peer_id, &cache_manager).await;

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
    cluster_actor.start_rebalance(peer_id, &cache_manager).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_some());
    let msg = buf.lock().await.pop_front();
    assert!(msg.is_some());
    let hb = msg.unwrap();
    assert!(matches!(hb, QueryIO::ClusterHeartBeat(..)));

    let QueryIO::ClusterHeartBeat(hb) = hb else {
        panic!("Expected ClusterHeartBeat message");
    };
    assert!(hb.hashring.is_some());
    assert_eq!(cluster_actor.hash_ring.get_pnode_count(), 2);
    assert_eq!(cluster_actor.hash_ring, hb.hashring.unwrap());
}

#[tokio::test]
async fn test_schedule_migration_if_required_when_noplan_is_made() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;
    tokio::time::sleep(Duration::from_millis(1)).await; // sleep to make sure last_modified is updated

    // Create hash ring for coordinating node
    let hash_ring = HashRing::default();
    let coordinator_replid = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let hash_ring = hash_ring
        .add_partition_if_not_exists(coordinator_replid, PeerIdentifier::new("127.0.0.1", 5999))
        .unwrap();
    let hash_ring = hash_ring
        .add_partition_if_not_exists(
            heartbeat_receiving_actor.replication.replid.clone(),
            heartbeat_receiving_actor.replication.self_identifier(),
        )
        .unwrap();

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    heartbeat_receiving_actor
        .schedule_migration_if_required(Some(hash_ring.clone()), &cache_manager)
        .await;

    // THEN
    assert!(heartbeat_receiving_actor.pending_requests.is_none());
    assert_eq!(heartbeat_receiving_actor.hash_ring, hash_ring);
    assert_ne!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_given_hashring_is_same() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    heartbeat_receiving_actor
        .schedule_migration_if_required(
            Some(heartbeat_receiving_actor.hash_ring.clone()),
            &cache_manager,
        )
        .await;

    // THEN
    assert_eq!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_no_hashring_given() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    heartbeat_receiving_actor.schedule_migration_if_required(None, &cache_manager).await;

    // THEN
    assert_eq!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_last_modified_is_lower_than_its_own() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    let mut hash_ring = HashRing::default();
    hash_ring.last_modified = last_modified - 1;

    // WHEN
    let (_hwm, cache_manager) = cache_manager_create_helper();
    heartbeat_receiving_actor.schedule_migration_if_required(Some(hash_ring), &cache_manager).await;

    // THEN
    assert_eq!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
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
async fn test_receive_batch_success_path() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();

    let peer_replid = ReplicationId::Key("repl_id_for_other_node".to_string());
    let (message_buf, sender_peer_id) =
        cluster_actor.test_add_peer(6567, NodeKind::NonData, Some(peer_replid.clone()));
    cluster_actor.hash_ring = cluster_actor
        .hash_ring
        .add_partition_if_not_exists(peer_replid.clone(), sender_peer_id.clone())
        .unwrap();

    let cache_entries =
        cache_entries_create_helper(&[("success_key1", "value1"), ("success_key2", "value2")]);
    let migrate_batch = migration_batch_create_helper("success_test", cache_entries);

    // WHEN
    cluster_actor.receive_batch(migrate_batch, &cache_manager, sender_peer_id).await;

    // THEN
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_migration_batch_ack(&message_buf, "success_test", true).await;

    // Verify the keys were actually stored in the cache
    let retrieved_value1 = cache_manager.route_get("success_key1").await.unwrap();
    let retrieved_value2 = cache_manager.route_get("success_key2").await.unwrap();

    assert!(retrieved_value1.is_some());
    assert!(retrieved_value2.is_some());
    assert_eq!(retrieved_value1.unwrap().value(), "value1");
    assert_eq!(retrieved_value2.unwrap().value(), "value2");
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
    assert_migration_batch_ack(&message_buf, "validation_test", false).await;
}

#[tokio::test]
async fn test_receive_batch_empty_cache_entries() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_hwm, cache_manager) = cache_manager_create_helper();
    let (message_buf, sender_peer_id) = cluster_actor.test_add_peer(6568, NodeKind::NonData, None);

    let migrate_batch = migration_batch_create_helper("empty_test", vec![]);

    // WHEN
    cluster_actor.receive_batch(migrate_batch, &cache_manager, sender_peer_id).await;

    // THEN
    assert_migration_batch_ack(&message_buf, "empty_test", true).await;
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
    let (migration_tx, _migration_rx) = tokio::sync::oneshot::channel();
    let batch_id = BatchId("test_batch".into());
    cluster_actor.pending_migrations.as_mut().unwrap().insert(batch_id, migration_tx);

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

    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let batch_id = BatchId("failure_batch".into());

    // Ensure pending_migrations is set up
    assert!(cluster_actor.pending_migrations.is_some());
    cluster_actor.pending_migrations.as_mut().unwrap().insert(batch_id.clone(), callback_tx);

    let ack = MigrationBatchAck::new_with_reject(batch_id.clone());

    // WHEN
    let result = cluster_actor.handle_migration_ack(ack).await;

    // THEN
    assert!(result.is_some()); // Method should return Some(())

    // Verify callback was called with error
    let callback_result = callback_rx.await.unwrap();
    assert!(callback_result.is_err());
    assert!(
        callback_result
            .unwrap_err()
            .to_string()
            .contains("Failed to send migration completion signal for batch failure_batch")
    );

    // Verify migrations are completed - pending_migrations should be None when all migrations are done
    assert!(cluster_actor.pending_migrations.is_none());
}

#[tokio::test]
async fn test_handle_migration_ack_batch_id_not_found() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(1).await;

    let (callback_tx, _callback_rx) = tokio::sync::oneshot::channel();
    let existing_batch_id = BatchId("existing_batch".into());
    cluster_actor.pending_migrations.as_mut().unwrap().insert(existing_batch_id, callback_tx);

    let non_existent_batch_id = BatchId("non_existent_batch".into());
    let ack = MigrationBatchAck { batch_id: non_existent_batch_id, success: true };

    // WHEN
    let result = cluster_actor.handle_migration_ack(ack).await;

    // THEN
    assert!(result.is_none()); // Method should return None when batch ID is not found

    // Verify existing batch is still there
    assert_eq!(cluster_actor.pending_migrations.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn test_handle_migration_ack_success_case_with_pending_reqs_and_migration() {
    // GIVEN
    let mut cluster_actor = setup_blocked_cluster_actor_with_requests(2).await;

    // Add the last pending migration
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let batch_id = BatchId("last_batch".into());
    cluster_actor.pending_migrations.as_mut().unwrap().insert(batch_id.clone(), callback_tx);

    let ack = MigrationBatchAck { batch_id, success: true };

    // Verify initially blocked
    assert!(cluster_actor.pending_requests.is_some());
    assert_eq!(cluster_actor.pending_requests.as_ref().unwrap().len(), 2);
    assert!(cluster_actor.pending_migrations.is_some());
    assert_eq!(cluster_actor.pending_migrations.as_ref().unwrap().len(), 1);

    // WHEN
    let result = cluster_actor.handle_migration_ack(ack).await;

    // THEN
    assert!(result.is_some());

    // Verify callback was successful
    let callback_result = callback_rx.await.unwrap();
    assert!(callback_result.is_ok());

    // Verify unblock_write_reqs_if_done was called and requests were unblocked
    // Since this was the last migration, both should be None now
    assert!(cluster_actor.pending_requests.is_none());
    assert!(cluster_actor.pending_migrations.is_none());
}

// Verify that the start_rebalance -> schedule_migration_if_required flow works end-to-end.
#[tokio::test]
async fn test_start_rebalance_schedules_migration_batches() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let keys = vec!["test_key_1".to_string(), "test_key_2".to_string()];
    let (_hwm, cache_manager) = cache_manager_create_helper_with_keys(keys).await;

    // Add a peer with a different replication ID that will trigger migration
    let target_repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let (target_peer_message_buf, peer_id) =
        cluster_actor.test_add_peer(6570, NodeKind::NonData, Some(target_repl_id.clone()));

    // Start the cluster actor in a background task so it can process scheduler messages
    let handler = cluster_actor.self_handler.clone();
    tokio::spawn(cluster_actor.handle(cache_manager.clone()));

    // WHEN - call start_rebalance which should trigger the full migration flow
    handler
        .send(ClusterCommand::Peer(PeerCommand {
            from: peer_id.clone(),
            msg: PeerMessage::StartRebalance,
        }))
        .await
        .unwrap();

    // Give some time for the async migration tasks to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // THEN - verify that the target peer received a heartbeat with the new hash ring
    let sent_messages = target_peer_message_buf.lock().await;

    assert!(!sent_messages.is_empty(), "Target peer should have received messages");

    // Look for ClusterHeartBeat messages
    let heartbeat_messages: Vec<_> = sent_messages
        .iter()
        .filter_map(|msg| if let QueryIO::ClusterHeartBeat(hb) = msg { Some(hb) } else { None })
        .collect();

    // Look for MigrateBatch messages
    let migrate_batch_messages: Vec<_> = sent_messages
        .iter()
        .filter_map(|msg| if let QueryIO::MigrateBatch(batch) = msg { Some(batch) } else { None })
        .collect();

    // Verify that a heartbeat with the new hash ring was sent
    assert!(!heartbeat_messages.is_empty(), "Target peer should have received heartbeat message");
    let heartbeat = heartbeat_messages.first().unwrap();
    assert!(heartbeat.hashring.is_some(), "Heartbeat should contain the new hash ring");
    assert_eq!(
        heartbeat.hashring.as_ref().unwrap().get_pnode_count(),
        2,
        "Hash ring should have 2 partitions"
    );

    assert!(
        !migrate_batch_messages.is_empty(),
        "Target peer should have received migrate batch message"
    );
}
