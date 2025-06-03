use std::sync::atomic::AtomicI32;

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
    let tasks = vec![migration_task_create_helper(0, 100), migration_task_create_helper(101, 102)];

    // WHEN - first being number of keys, second being number of batches
    let atom = Arc::new((AtomicI32::new(0), AtomicI32::new(0)));

    tokio::spawn({
        let atom = atom.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                // ! First check, message must be Scheduler Message
                let ClusterCommand::Scheduler(SchedulerMessage::MigrateBatchKeys(batch, tx)) = msg
                else {
                    panic!()
                };
                batch.tasks.iter().for_each(|task| {
                    atom.0.fetch_add(task.keys_to_migrate.len() as i32, Ordering::Relaxed);
                    atom.1.fetch_add(1, Ordering::Relaxed);
                });
                let _ = tx.send(Ok(()));
            }
        }
    });

    ClusterActor::<MemoryOpLogs>::schedule_migrations(fake_handler, tasks).await;

    while atom.0.load(Ordering::Relaxed) != 101 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(atom.1.load(Ordering::Relaxed), 2);
}
