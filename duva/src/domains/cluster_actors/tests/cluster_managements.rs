use super::*;

#[tokio::test]
async fn test_cluster_nodes() {
    use std::io::Write;

    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    cluster_actor.replication.hwm.store(15, Ordering::Release);

    let repl_id = cluster_actor.replication.replid.clone();

    // followers
    for port in [6379, 6380] {
        let (key, peer) = create_peer_helper(
            cluster_actor.self_handler.clone(),
            cluster_actor.replication.hwm.load(Ordering::Relaxed),
            &repl_id,
            port,
            NodeKind::Replica,
            FakeReadWrite::new(),
        );
        cluster_actor.members.insert(key.clone(), peer);
    }

    // leader for different shard
    let second_shard_repl_id = ReplicationId::Key(uuid::Uuid::now_v7().to_string());
    let second_shard_leader_port = rand::random::<u16>();
    let (second_shard_leader_identifier, second_peer) = create_peer_helper(
        cluster_actor.self_handler.clone(),
        0,
        &second_shard_repl_id,
        second_shard_leader_port,
        NodeKind::NonData,
        FakeReadWrite::new(),
    );

    cluster_actor.members.insert(second_shard_leader_identifier.clone(), second_peer);

    // follower for different shard
    for port in [2655, 2653] {
        let (key, peer) = create_peer_helper(
            cluster_actor.self_handler.clone(),
            0,
            &second_shard_repl_id,
            port,
            NodeKind::NonData,
            FakeReadWrite::new(),
        );
        cluster_actor.members.insert(key, peer);
    }

    // WHEN
    let res = cluster_actor.cluster_nodes();
    let repl_id = cluster_actor.replication.replid.clone();
    assert_eq!(res.len(), 6);

    let file_content = format!(
        r#"
        127.0.0.1:6379 {repl_id} 0 15
        127.0.0.1:6380 {repl_id} 0 15
        {second_shard_leader_identifier} {second_shard_repl_id} 0 0
        127.0.0.1:2655 {second_shard_repl_id} 0 0
        127.0.0.1:2653 {second_shard_repl_id} 0 0
        localhost:8080 myself,{repl_id} 0 15
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
    cluster_actor.test_snapshot_topology().await.unwrap();

    // THEN
    let topology = tokio::fs::read_to_string(path).await.unwrap();
    let expected_topology = format!("{} myself,{} 0 {}", self_id, repl_id, hwm);
    assert_eq!(topology, expected_topology);

    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn test_snapshot_topology_after_add_peer() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let path = "test_snapshot_topology_after_add_peer.tp";
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
        ),
        kill_switch,
    );

    // WHEN
    cluster_actor.add_peer(peer).await;
    cluster_actor.test_snapshot_topology().await.unwrap();

    // THEN
    let topology = tokio::fs::read_to_string(path).await.unwrap();
    let mut cluster_nodes = topology.split("\r\n").map(|x| x.to_string()).collect::<Vec<String>>();

    cluster_nodes.dedup();
    assert_eq!(cluster_nodes.len(), 2);

    let hwm = cluster_actor.replication.hwm.load(Ordering::Relaxed);

    for value in [
        format!("127.0.0.1:3849 {} 0 {}", repl_id, hwm),
        format!("{} myself,{} 0 {}", cluster_actor.replication.self_identifier(), repl_id, hwm),
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
        .test_join_peer_network_if_absent(vec![PeerState::new(
            &format!("127.0.0.1:{}", bind_addr.port() - 10000),
            0,
            cluster_actor.replication.replid.clone(),
            NodeKind::Replica,
        )])
        .await;

    assert!(handle.await.is_ok());
    assert!(rx.await.is_ok());
}
