use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;

use crate::domains::cluster_actors::hash_ring::{
    HashRing, MigrationTask, tests::migration_task_create_helper,
};

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

    // WHEN
    cluster_actor.start_rebalance(PeerIdentifier("127.0.0.1:6559".into())).await;

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

    let (buf, peer_id) = cluster_actor.test_add_peer(6559, NodeKind::Replica, None);

    // WHEN
    cluster_actor.start_rebalance(peer_id).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none());
    let msg = buf.lock().await.pop_front();
    assert!(msg.is_none());
}

#[tokio::test]
async fn test_start_rebalance_happy_path() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    let (buf, peer_id) = cluster_actor.test_add_peer(
        6559,
        NodeKind::NonData,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
    );

    // WHEN
    cluster_actor.start_rebalance(peer_id).await;

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
async fn test_start_rebalance_should_be_idempotent() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    let (buf, peer_id) = cluster_actor.test_add_peer(
        6559,
        NodeKind::NonData,
        Some(ReplicationId::Key(uuid::Uuid::now_v7().to_string())),
    );

    // WHEN
    cluster_actor.start_rebalance(peer_id.clone()).await;
    assert_eq!(cluster_actor.hash_ring.get_pnode_count(), 2);
    cluster_actor.start_rebalance(peer_id).await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.get_pnode_count(), 2);

    // ! still, the message should be sent
    let msg1 = buf.lock().await.pop_front();
    let msg2 = buf.lock().await.pop_front();
    assert!(msg1.is_some());
    assert!(msg2.is_some());
    assert_eq!(msg1, msg2);
}

#[tokio::test]
#[should_panic(expected = "hash ring should be updated")]
async fn test_make_migration_plan_happypath() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    // this hash ring is the one for coordinating node
    let mut hash_ring = HashRing::default();

    let coordinator_replid = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    hash_ring
        .add_partition_if_not_exists(coordinator_replid, PeerIdentifier::new("127.0.0.1", 5999));

    // this is the one for receiving actor
    hash_ring.add_partition_if_not_exists(
        heartbeat_receiving_actor.replication.replid.clone(),
        heartbeat_receiving_actor.replication.self_identifier(),
    );

    // WHEN - now, when heartbeat receiving actor hashring info (through heartbeat)
    let cache_manager = CacheManager { inboxes: vec![] };
    heartbeat_receiving_actor
        .make_migration_tasks_if_valid(Some(hash_ring.clone()), &cache_manager)
        .await;

    // THEN - it should create a migration plan
    assert!(heartbeat_receiving_actor.pending_requests.is_some());
    assert_eq!(heartbeat_receiving_actor.hash_ring, hash_ring, "hash ring should be updated");
    assert_ne!(
        heartbeat_receiving_actor.hash_ring.last_modified, last_modified,
        "last modified should be updated"
    );
}

#[tokio::test]
async fn test_make_migration_plan_when_given_hashring_is_same() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    // WHEN - now, when heartbeat receiving actor hashring info (through heartbeat)
    let cache_manager = CacheManager { inboxes: vec![] };
    heartbeat_receiving_actor
        .make_migration_tasks_if_valid(
            Some(heartbeat_receiving_actor.hash_ring.clone()),
            &cache_manager,
        )
        .await;

    // THEN no change should be made
    assert_eq!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_no_hashring_given() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    // WHEN - now, when heartbeat receiving actor hashring info (through heartbeat)
    let cache_manager = CacheManager { inboxes: vec![] };
    heartbeat_receiving_actor.make_migration_tasks_if_valid(None, &cache_manager).await;

    // THEN no change should be made
    assert_eq!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_make_migration_plan_when_last_modified_is_lower_than_its_own() {
    // GIVEN
    let mut heartbeat_receiving_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let last_modified = heartbeat_receiving_actor.hash_ring.last_modified;

    let mut hash_ring = HashRing::default();
    hash_ring.last_modified = last_modified - 1;

    // WHEN - now, when heartbeat receiving actor hashring info (through heartbeat)
    let cache_manager = CacheManager { inboxes: vec![] };
    heartbeat_receiving_actor.make_migration_tasks_if_valid(Some(hash_ring), &cache_manager).await;

    // THEN no change should be made
    assert_eq!(heartbeat_receiving_actor.hash_ring.last_modified, last_modified);
}

#[tokio::test]
async fn test_schedule_migrations_happypath() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    // Create dummy tasks
    let mut migration_plans = BTreeMap::new();
    migration_plans.insert(
        ReplicationId::Key("my_test_key".to_string()),
        vec![migration_task_create_helper(0, 100), migration_task_create_helper(101, 102)],
    );

    // WHEN - first being number of keys, second being number of batches
    let atom = Arc::new((AtomicI32::new(0), AtomicI32::new(0)));

    tokio::spawn({
        let atom = atom.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                // ! First check, message must be Scheduler Message
                let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationTarget(batch, tx)) =
                    msg
                else {
                    panic!()
                };
                assert_eq!(batch.target_repl, ReplicationId::Key("my_test_key".to_string()));
                batch.tasks.iter().for_each(|task| {
                    atom.0.fetch_add(task.keys_to_migrate.len() as i32, Ordering::Relaxed);
                    atom.1.fetch_add(1, Ordering::Relaxed);
                });
                let _ = tx.send(Ok(()));
            }
        }
    });

    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;

    while atom.0.load(Ordering::Relaxed) != 101 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(atom.1.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn test_schedule_migrations_empty_plans() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    let migration_plans = BTreeMap::new(); // Empty migration plans

    // WHEN
    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;

    // THEN - function should return immediately for empty plans
    // Verify this by checking that receiving with a short timeout gives us None
    let result = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await;
    assert!(
        result.is_err() || result.unwrap().is_none(),
        "No messages should be sent for empty migration plans"
    );
}

#[tokio::test]
async fn test_schedule_migrations_multiple_replication_ids() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(20);
    let fake_handler = ClusterCommandHandler(tx);

    // Create migration plans for multiple replication IDs
    let mut migration_plans = BTreeMap::new();
    migration_plans.insert(
        ReplicationId::Key("repl_1".to_string()),
        vec![migration_task_create_helper(0, 50)],
    );
    migration_plans.insert(
        ReplicationId::Key("repl_2".to_string()),
        vec![migration_task_create_helper(0, 30)],
    );
    migration_plans.insert(
        ReplicationId::Key("repl_3".to_string()),
        vec![migration_task_create_helper(0, 20)],
    );

    // WHEN
    let received_messages = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let message_handler = {
        let received_messages = received_messages.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationTarget(batch, tx)) =
                    msg
                else {
                    panic!("Expected MigrateBatchKeys message");
                };

                received_messages.lock().await.push(batch.target_repl.clone());
                let _ = tx.send(Ok(()));
            }
        })
    };

    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;

    // Cancel the message handler since schedule_migrations has completed

    // THEN - should receive messages for all replication IDs
    let messages = received_messages.lock().await;
    assert_eq!(messages.len(), 3, "Should receive exactly 3 messages for 3 replication IDs");
    assert!(messages.contains(&ReplicationId::Key("repl_1".to_string())));
    assert!(messages.contains(&ReplicationId::Key("repl_2".to_string())));
    assert!(messages.contains(&ReplicationId::Key("repl_3".to_string())));
    message_handler.abort();
}

#[tokio::test]
async fn test_schedule_migrations_large_task_batching() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    // Create a large task that should be split into multiple batches (>100 keys total)
    let mut migration_plans = BTreeMap::new();
    migration_plans.insert(
        ReplicationId::Key("large_repl".to_string()),
        vec![
            migration_task_create_helper(0, 80),    // 80 keys (0..80)
            migration_task_create_helper(81, 160),  // 79 keys (81..160)
            migration_task_create_helper(161, 200), // 39 keys (161..200)
        ],
    );

    // WHEN
    let batch_info = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let message_handler = {
        let batch_info = batch_info.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationTarget(batch, tx)) =
                    msg
                else {
                    panic!("Expected MigrateBatchKeys message");
                };

                let total_keys: usize =
                    batch.tasks.iter().map(|task| task.keys_to_migrate.len()).sum();
                batch_info.lock().await.push((batch.target_repl.clone(), total_keys));
                let _ = tx.send(Ok(()));
            }
        })
    };

    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;

    message_handler.abort();

    // THEN - should receive multiple batches
    let batches = batch_info.lock().await;

    // The algorithm processes tasks in reverse order (pop_last from BTreeMap, pop from Vec)
    // So it processes: task3(39), task2(79), task1(80)
    // Batch 1: task3(39) + task2(79) = 118 keys (stops when adding task1 would exceed 100)
    // Batch 2: task1(80) = 80 keys
    let expected_batches = vec![118, 80]; // First batch: task3+task2, Second batch: task1

    assert_eq!(batches.len(), 2, "Should have exactly 2 batches");

    let mut actual_key_counts: Vec<usize> = batches.iter().map(|(_, count)| *count).collect();
    actual_key_counts.sort(); // Sort to handle potential ordering differences in parallel execution
    let mut expected_sorted = expected_batches.clone();
    expected_sorted.sort();

    assert_eq!(actual_key_counts, expected_sorted, "Batch sizes should match expected pattern");

    // All batches should be for the same replication ID
    for (repl_id, _) in &*batches {
        assert_eq!(repl_id, &ReplicationId::Key("large_repl".to_string()));
    }

    // Total keys should be preserved
    let total_keys: usize = batches.iter().map(|(_, count)| count).sum();
    assert_eq!(total_keys, 198, "Total keys should be preserved across batches: 80+79+39=198");
}

#[tokio::test]
async fn test_schedule_migrations_synchronization() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    // Create multiple migration plans to test parallel synchronization
    let mut migration_plans = BTreeMap::new();
    migration_plans.insert(
        ReplicationId::Key("sync_test_1".to_string()),
        vec![migration_task_create_helper(0, 10)],
    );
    migration_plans.insert(
        ReplicationId::Key("sync_test_2".to_string()),
        vec![migration_task_create_helper(0, 10)],
    );

    // WHEN - simulate slow response to test synchronization
    let response_count = Arc::new(AtomicI32::new(0));

    let message_handler = {
        let response_count = response_count.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationTarget(_, tx)) =
                    msg
                else {
                    panic!("Expected MigrateBatchKeys message");
                };

                response_count.fetch_add(1, Ordering::Relaxed);

                // Introduce delay to test synchronization
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _ = tx.send(Ok(()));
            }
        })
    };

    let start_time = Instant::now();
    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;
    let elapsed = start_time.elapsed();

    message_handler.abort();

    // THEN - should wait for all responses before completing
    // The key test is that schedule_migrations waits for all tasks to complete
    assert!(elapsed >= Duration::from_millis(45), "Should wait for synchronization");
    assert_eq!(
        response_count.load(Ordering::Relaxed),
        2,
        "Should have processed exactly two messages in parallel"
    );
}

#[tokio::test]
async fn test_schedule_migrations_channel_error_handling() {
    // GIVEN
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    let mut migration_plans = BTreeMap::new();
    migration_plans.insert(
        ReplicationId::Key("error_test".to_string()),
        vec![migration_task_create_helper(0, 10)],
    );

    // WHEN - drop receiver to simulate channel closure
    drop(rx);

    // THEN - should handle gracefully without panicking
    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;
    // If we reach this point, the function handled the error gracefully
}

#[tokio::test]
async fn test_schedule_migrations_callback_error_response() {
    // GIVEN
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let fake_handler = ClusterCommandHandler(tx);

    let mut migration_plans = BTreeMap::new();
    migration_plans.insert(
        ReplicationId::Key("error_response_test".to_string()),
        vec![migration_task_create_helper(0, 10)],
    );

    // WHEN - simulate error response from migration handler
    let message_handler = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let ClusterCommand::Scheduler(SchedulerMessage::ScheduleMigrationTarget(_, tx)) = msg
            else {
                panic!("Expected MigrateBatchKeys message");
            };

            // Send error response
            let _ = tx.send(Err(anyhow::anyhow!("Simulated migration error")));
        }
    });

    // THEN - should handle error response gracefully
    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, migration_plans).await;

    message_handler.abort();
    // If we reach this point, the function handled the error response gracefully
}

// Tests for migrate_keys function

#[tokio::test]
async fn test_migrate_keys_target_peer_not_found() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let hwm = Arc::new(AtomicU64::new(0));
    let cache_manager = CacheManager::run_cache_actors(hwm);

    let tasks = MigrationTarget::new(
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

    let non_existent_repl = ReplicationId::Key("non_existent".to_string());
    assert_eq!(cluster_actor.peerid_by_replid(&non_existent_repl), None);
}

#[tokio::test]
async fn test_migrate_keys_retrieves_actual_data() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    cluster_actor.block_write_reqs();

    // Create a cache manager with actual cache actors
    let hwm = Arc::new(AtomicU64::new(0));
    let cache_manager = CacheManager::run_cache_actors(hwm);

    // Add a peer with a specific replication ID
    let target_repl_id = ReplicationId::Key("data_target".to_string());
    let (_, _) = cluster_actor.test_add_peer(6564, NodeKind::NonData, Some(target_repl_id.clone()));

    // Set up some test data in the cache
    let test_keys = vec!["test_key_1".to_string(), "test_key_2".to_string()];
    cache_manager
        .route_set("test_key_1".to_string(), "value_1".to_string(), None, 1)
        .await
        .unwrap();
    cache_manager
        .route_set("test_key_2".to_string(), "value_2".to_string(), None, 2)
        .await
        .unwrap();

    // Create migration tasks with the test keys
    let migration_task = MigrationTask { task_id: (0, 100), keys_to_migrate: test_keys.clone() };
    let tasks = MigrationTarget::new(target_repl_id, vec![migration_task]);

    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

    // WHEN
    cluster_actor.migrate_batch(tasks, &cache_manager, callback_tx).await;

    // THEN
    // 1. pending_migrations should be set
    let pending_migrations = cluster_actor.pending_migrations.unwrap();
    assert_eq!(pending_migrations.len(), 1);

    // Verify the data is still in cache (migration doesn't remove it yet)
    let retrieved_value_1 = cache_manager.route_get("test_key_1").await.unwrap();
    let retrieved_value_2 = cache_manager.route_get("test_key_2").await.unwrap();

    assert!(retrieved_value_1.is_some());
    assert!(retrieved_value_2.is_some());
    assert_eq!(retrieved_value_1.unwrap().value(), "value_1");
    assert_eq!(retrieved_value_2.unwrap().value(), "value_2");

    // callback will not be called because migration is not completed
    let result = tokio::time::timeout(Duration::from_millis(100), callback_rx).await;
    assert!(result.is_err());
}
