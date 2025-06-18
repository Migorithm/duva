use tokio::sync::RwLock;

use super::*;

#[tokio::test]
async fn test_cluster_nodes() {
    use std::io::Write;

    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;

    // followers
    for port in [6379, 6380] {
        cluster_actor.test_add_peer(port, NodeKind::Replica, None);
    }

    // leader for different shard
    let second_shard_repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let second_shard_leader_port = rand::random::<u16>();

    let (_, second_shard_leader_identifier) = cluster_actor.test_add_peer(
        second_shard_leader_port,
        NodeKind::NonData,
        Some(second_shard_repl_id.clone()),
    );

    // follower for different shard
    for port in [2655, 2653] {
        cluster_actor.test_add_peer(port, NodeKind::NonData, Some(second_shard_repl_id.clone()));
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
    cluster_actor.topology_writer = tokio::fs::File::create(path).await.unwrap();

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
async fn snapshot_topology_after_add_peer() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let path = "snapshot_topology_after_add_peer.tp";
    cluster_actor.topology_writer = tokio::fs::File::create(path).await.unwrap();

    let repl_id = cluster_actor.replication.replid.clone();

    let fake_buf = FakeReadWrite::new();
    let kill_switch = PeerListener::spawn(
        fake_buf.clone(),
        cluster_actor.self_handler.clone(),
        PeerIdentifier("127.0.0.1:3849".into()),
    );

    let peer = Peer::new(
        fake_buf,
        PeerState::new(
            "127.0.0.1:3849",
            0,
            ReplicationId::Key(repl_id.to_string()),
            NodeKind::Replica,
            ReplicationRole::Follower,
        ),
        kill_switch,
    );

    // WHEN
    cluster_actor.add_peer(peer).await;
    cluster_actor.snapshot_topology().await.unwrap();

    // THEN
    let topology = tokio::fs::read_to_string(path).await.unwrap();
    let mut cluster_nodes = topology.split("\r\n").map(|x| x.to_string()).collect::<Vec<String>>();

    cluster_nodes.dedup();
    assert_eq!(cluster_nodes.len(), 2);

    let hwm = cluster_actor.replication.hwm.load(Ordering::Relaxed);

    for value in [
        format!("127.0.0.1:3849 {} 0 {} {}", repl_id, hwm, ReplicationRole::Follower),
        format!(
            "{} myself,{} 0 {} {}",
            cluster_actor.replication.self_identifier(),
            repl_id,
            hwm,
            ReplicationRole::Leader
        ),
    ] {
        assert!(cluster_nodes.contains(&value));
    }

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
    replication_state.is_leader_mode = false;

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
            NodeKind::Replica,
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
        .add_partition_if_not_exists(replid.clone(), PeerIdentifier("peer1".into()))
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
