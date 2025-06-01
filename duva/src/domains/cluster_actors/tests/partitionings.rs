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
    let buf = FakeReadWrite::new();
    let (peer_id, peer) = create_peer_helper(
        cluster_actor.self_handler.clone(),
        0,
        &cluster_actor.replication.replid,
        6559,
        NodeKind::Replica,
        buf.clone(),
    );
    cluster_actor.members.insert(peer_id, peer);

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to.clone(), lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none());

    let msg = buf.0.lock().await.pop_front();
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
    let buf = FakeReadWrite::new();
    let (peer_id, peer) = create_peer_helper(
        cluster_actor.self_handler.clone(),
        0,
        &cluster_actor.replication.replid,
        6559,
        NodeKind::NonData,
        buf.clone(),
    );
    cluster_actor.members.insert(peer_id, peer);

    // WHEN
    let request_to = PeerIdentifier("127.0.0.1:6559".into());
    let lazy_o = LazyOption::Eager;
    cluster_actor.rebalance_request(request_to.clone(), lazy_o).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_some());

    let msg = buf.0.lock().await.pop_front();
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
    let buf = FakeReadWrite::new();
    let (peer_id, peer) = create_peer_helper(
        cluster_actor.self_handler.clone(),
        0,
        &cluster_actor.replication.replid.clone(),
        6559,
        NodeKind::Replica,
        buf.clone(),
    );

    cluster_actor.add_peer(peer).await;

    // WHEN
    cluster_actor.start_rebalance(peer_id).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_none());
    let msg = buf.0.lock().await.pop_front();
    assert!(msg.is_none());
}

#[tokio::test]
async fn test_start_rebalance_happy_path() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let buf = FakeReadWrite::new();
    let (peer_id, peer) = create_peer_helper(
        cluster_actor.self_handler.clone(),
        0,
        &ReplicationId::Key(uuid::Uuid::now_v7().to_string()), // non data
        6559,
        NodeKind::NonData,
        buf.clone(),
    );

    cluster_actor.add_peer(peer).await;

    // WHEN
    cluster_actor.start_rebalance(peer_id).await;

    // THEN
    assert!(cluster_actor.pending_requests.is_some());
    let msg = buf.0.lock().await.pop_front();
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
    let buf = FakeReadWrite::new();
    let (peer_id, peer) = create_peer_helper(
        cluster_actor.self_handler.clone(),
        0,
        &ReplicationId::Key(uuid::Uuid::now_v7().to_string()), // non data
        6559,
        NodeKind::NonData,
        buf.clone(),
    );

    cluster_actor.add_peer(peer).await;
    // WHEN
    cluster_actor.start_rebalance(peer_id.clone()).await;
    assert_eq!(cluster_actor.hash_ring.get_pnode_count(), 2);
    cluster_actor.start_rebalance(peer_id).await;

    // THEN
    assert_eq!(cluster_actor.hash_ring.get_pnode_count(), 2);

    // ! still, the message should be sent
    let msg1 = buf.0.lock().await.pop_front();
    let msg2 = buf.0.lock().await.pop_front();
    assert!(msg1.is_some());
    assert!(msg2.is_some());
    assert_eq!(msg1, msg2);
}
