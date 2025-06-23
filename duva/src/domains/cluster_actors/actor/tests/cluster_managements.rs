use tokio::sync::RwLock;

use super::*;

#[tokio::test]
async fn test_cluster_nodes() {
    use std::io::Write;

    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    // followers
    for port in [6379, 6380] {
        cluster_actor.test_add_peer(port, None, false);
    }

    // leader for different shard
    let second_shard_repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let second_shard_leader_port = rand::random::<u16>();

    let (_, second_shard_leader_identifier) = cluster_actor.test_add_peer(
        second_shard_leader_port,
        Some(second_shard_repl_id.clone()),
        true,
    );

    // follower for different shard
    for port in [2655, 2653] {
        cluster_actor.test_add_peer(port, Some(second_shard_repl_id.clone()), false);
    }

    // WHEN
    let res = cluster_actor.cluster_nodes();
    let repl_id = cluster_actor.replication.replid.clone();
    assert_eq!(res.len(), 6);

    let file_content = format!(
        r#"
        127.0.0.1:6379 {repl_id} 0 0
        127.0.0.1:6380 {repl_id} 0 0
        {second_shard_leader_identifier} {second_shard_repl_id} 0 0
        127.0.0.1:2655 {second_shard_repl_id} 0 0
        127.0.0.1:2653 {second_shard_repl_id} 0 0
        localhost:8080 myself,{repl_id} 0 0
        "#
    );

    let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    write!(temp_file, "{}", file_content).expect("Failed to write to temp file");
    let nodes = PeerState::from_file(temp_file.path().to_str().unwrap());

    for value in nodes {
        assert!(res.contains(&value));
    }
}

#[tokio::test]
async fn test_store_current_topology() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let path = "test_store_current_topology.tp";
    cluster_actor.topology_writer = std::fs::File::create(path).unwrap();

    let repl_id = cluster_actor.replication.replid.clone();
    let self_id = cluster_actor.replication.self_identifier();
    let hwm = cluster_actor.replication.hwm.load(Ordering::Relaxed);

    // WHEN
    cluster_actor.snapshot_topology().await.unwrap();

    // THEN
    let topology = tokio::fs::read_to_string(path).await.unwrap();
    let expected_topology =
        format!("{} myself,{} 0 {} {}", self_id, repl_id, hwm, ReplicationRole::Leader);
    assert_eq!(topology, expected_topology);

    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn test_reconnection_on_gossip() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    // * run listener to see if connection is attempted
    let listener = TcpListener::bind("127.0.0.1:44455").await.unwrap(); // ! Beaware that this is cluster port
    let bind_addr = listener.local_addr().unwrap();

    let mut replication_state = cluster_actor.replication.clone();
    replication_state.role = ReplicationRole::Follower;

    let (tx, rx) = tokio::sync::oneshot::channel();

    // Spawn the listener task
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut inbound_stream = InboundStream::new(stream, replication_state.clone());
        if inbound_stream.recv_handshake().await.is_ok() {
            let _ = tx.send(());
        };
    });

    // WHEN - try to reconnect
    cluster_actor
        .join_peer_network_if_absent(vec![PeerState::new(
            &format!("127.0.0.1:{}", bind_addr.port() - 10000),
            0,
            cluster_actor.replication.replid.clone(),
            ReplicationRole::Follower,
        )])
        .await;

    assert!(handle.await.is_ok());
    assert!(rx.await.is_ok());
}

#[tokio::test]
async fn test_topology_broadcast_on_hash_ring_change() {
    // GIVEN
    let topology = Arc::new(RwLock::new(Topology::default()));
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let replid = ReplicationId::Key("node1".into());

    // Setup old ring
    let hash_ring = HashRing::default()
        .add_partitions_if_not_exist(vec![(replid.clone(), PeerIdentifier("peer1".into()))])
        .unwrap();

    cluster_actor.hash_ring = hash_ring.clone();

    let mut subscriber = cluster_actor.node_change_broadcast.subscribe();

    let task = tokio::spawn({
        let topology = topology.clone();
        async move {
            while let Ok(tp) = subscriber.recv().await {
                let mut guard = topology.write().await;
                *guard = tp;
                break;
            }
        }
    });

    cluster_actor.block_write_reqs();

    // WHEN
    cluster_actor.unblock_write_reqs_if_done();
    let _ = task.await;

    // THEN
    let guard = topology.read().await;
    assert_eq!(guard.hash_ring, hash_ring)
}

#[tokio::test]
async fn test_update_cluster_members_updates_fields() {
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (_, peer_id) = cluster_actor.test_add_peer(6379, None, false);
    let initial = cluster_actor.members.get(&peer_id).unwrap();

    let initial_last_seen = initial.last_seen;
    let initial_role = initial.state().role.clone();

    let cluster_nodes = vec![PeerState::new(
        &peer_id,
        100,
        cluster_actor.replication.replid.clone(),
        ReplicationRole::Leader,
    )];
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    cluster_actor.update_cluster_members(&peer_id, 123, &cluster_nodes).await;

    let updated = cluster_actor.members.get(&peer_id).unwrap();
    assert_eq!(updated.match_index(), 123);
    assert_eq!(updated.state().role, ReplicationRole::Leader);
    assert_ne!(updated.state().role, initial_role);
    assert!(updated.last_seen > initial_last_seen);
}

#[tokio::test]
async fn test_shard_leaders() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    // Create different replication IDs for different shards
    let shard1_replid = ReplicationId::Key("shard1".into());
    let shard2_replid = ReplicationId::Key("shard2".into());
    let shard3_replid = ReplicationId::Key("shard3".into());

    // Add followers (should not be included in shard_leaders)
    for port in [6379, 6380] {
        cluster_actor.test_add_peer(port, Some(shard1_replid.clone()), false);
    }

    // Add leaders for different shards
    let (_, leader1_id) = cluster_actor.test_add_peer(7001, Some(shard1_replid.clone()), true);
    let (_, leader2_id) = cluster_actor.test_add_peer(7002, Some(shard2_replid.clone()), true);
    let (_, leader3_id) = cluster_actor.test_add_peer(7003, Some(shard3_replid.clone()), true);

    // WHEN
    let shard_leaders = cluster_actor.shard_leaders();

    // THEN
    assert_eq!(shard_leaders.len(), 4); // plus 1 for self leader

    // Verify all expected leaders are present
    let expected_leaders = vec![
        (shard1_replid.clone(), leader1_id.clone()),
        (shard2_replid.clone(), leader2_id.clone()),
        (shard3_replid.clone(), leader3_id.clone()),
    ];

    for expected_leader in expected_leaders {
        assert!(
            shard_leaders.contains(&expected_leader),
            "Expected leader {:?} not found in shard_leaders",
            expected_leader
        );
    }
}
