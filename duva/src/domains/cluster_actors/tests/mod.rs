mod elections;
mod replications;
#[allow(unused_variables)]
use super::actor::ClusterCommandHandler;
use super::session::SessionRequest;
use super::*;
use crate::CacheManager;
use crate::NodeKind;
use crate::ReplicationId;
use crate::ReplicationState;
use crate::adapters::op_logs::memory_based::MemoryOpLogs;
use crate::domains::IoError;
use crate::domains::TRead;
use crate::domains::TWrite;
use crate::domains::caches::actor::CacheCommandSender;
use crate::domains::caches::command::CacheCommand;
use crate::domains::cluster_actors::replication::ReplicationRole;
use crate::domains::operation_logs::WriteOperation;
use crate::domains::operation_logs::WriteRequest;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::RejectionReason;
use crate::domains::peers::command::ReplicationAck;
use crate::domains::peers::connections::connection_types::ReadConnected;
use crate::domains::peers::connections::inbound::stream::InboundStream;
use crate::domains::peers::peer::PeerState;
use crate::domains::peers::service::PeerListener;
use crate::domains::query_parsers::QueryIO;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs::OpenOptions;
use tokio::net::TcpListener;

use tokio::sync::Mutex;
use tokio::sync::mpsc::channel;
use tokio::time::timeout;
use uuid::Uuid;

fn write_operation_create_helper(
    index_num: u64,
    term: u64,
    key: &str,
    value: &str,
) -> WriteOperation {
    WriteOperation {
        log_index: index_num.into(),
        request: WriteRequest::Set { key: key.into(), value: value.into(), expires_at: None },
        term,
    }
}

fn heartbeat_create_helper(term: u64, hwm: u64, op_logs: Vec<WriteOperation>) -> HeartBeat {
    HeartBeat {
        term,
        hwm,
        prev_log_index: if !op_logs.is_empty() { op_logs[0].log_index - 1 } else { 0 },
        prev_log_term: 0,
        append_entries: op_logs,
        ban_list: vec![],
        from: PeerIdentifier::new("localhost", 8080),
        replid: ReplicationId::Key("localhost".to_string().into()),
        hop_count: 0,
        cluster_nodes: vec![],
        hashring: None,
    }
}

fn create_peer_helper(
    cluster_sender: ClusterCommandHandler,
    hwm: u64,
    repl_id: &ReplicationId,
    port: u16,
    node_kind: NodeKind,
    fake_buf: FakeReadWrite,
) -> (PeerIdentifier, Peer) {
    let key = PeerIdentifier::new("127.0.0.1", port);

    let kill_switch = PeerListener::spawn(fake_buf.clone(), cluster_sender, key.clone());
    let peer =
        Peer::new(fake_buf, PeerState::new(&key, hwm, repl_id.clone(), node_kind), kill_switch);
    (key, peer)
}
pub async fn cluster_actor_create_helper() -> ClusterActor<MemoryOpLogs> {
    let replication = ReplicationState::new(
        ReplicationId::Key("master".into()),
        ReplicationRole::Leader,
        "localhost",
        8080,
        0,
    );
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("duva.tp");

    let topology_writer =
        OpenOptions::new().create(true).write(true).truncate(true).open(path).await.unwrap();

    ClusterActor::new(100, replication, 100, topology_writer, MemoryOpLogs::default())
}

#[derive(Debug, Clone)]
struct FakeReadWrite(Arc<Mutex<VecDeque<QueryIO>>>);

impl FakeReadWrite {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(VecDeque::new())))
    }
}

#[async_trait::async_trait]
impl TWrite for FakeReadWrite {
    async fn write(&mut self, io: QueryIO) -> Result<(), IoError> {
        let mut guard = self.0.lock().await;
        guard.push_back(io);
        Ok(())
    }
}

#[async_trait::async_trait]
impl TRead for FakeReadWrite {
    async fn read_bytes(&mut self, buf: &mut BytesMut) -> Result<(), IoError> {
        Ok(())
    }

    async fn read_values(&mut self) -> Result<Vec<QueryIO>, IoError> {
        let mut values = Vec::new();
        let mut guard = self.0.lock().await;

        values.extend(guard.drain(..));
        Ok(values)
    }
}

fn cluster_member_create_helper(
    actor: &mut ClusterActor<MemoryOpLogs>,
    fake_bufs: Vec<FakeReadWrite>,
    cluster_sender: ClusterCommandHandler,

    follower_hwm: u64,
) {
    for fake_b in fake_bufs.into_iter() {
        let port = rand::random::<u16>();
        let key = PeerIdentifier::new("localhost", port);

        let kill_switch = PeerListener::spawn(
            ReadConnected(Box::new(fake_b.clone())),
            cluster_sender.clone(),
            key.clone(),
        );
        actor.members.insert(
            PeerIdentifier::new("localhost", port),
            Peer::new(
                fake_b.clone(),
                PeerState::new(
                    &format!("localhost:{}", port),
                    follower_hwm,
                    ReplicationId::Key("localhost".to_string().into()),
                    NodeKind::Replica,
                ),
                kill_switch,
            ),
        );
    }
}

fn consensus_request_create_helper(
    tx: tokio::sync::oneshot::Sender<Result<ConsensusClientResponse, anyhow::Error>>,
    session_req: Option<SessionRequest>,
) -> ConsensusRequest {
    let consensus_request = ConsensusRequest::new(
        WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
        tx,
        session_req,
    );
    consensus_request
}

#[tokio::test]
async fn test_cluster_nodes() {
    use std::io::Write;

    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper().await;
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
    let mut cluster_actor = cluster_actor_create_helper().await;
    let path = "test_store_current_topology.tp";
    cluster_actor.topology_writer = tokio::fs::File::create(path).await.unwrap();

    let repl_id = cluster_actor.replication.replid.clone();
    let self_id = cluster_actor.replication.self_identifier();
    let hwm = cluster_actor.replication.hwm.load(Ordering::Relaxed);

    // WHEN
    cluster_actor.snapshot_topology().await.unwrap();

    // THEN
    let topology = tokio::fs::read_to_string(path).await.unwrap();
    let expected_topology = format!("{} myself,{} 0 {}", self_id, repl_id, hwm);
    assert_eq!(topology, expected_topology);

    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn test_snapshot_topology_after_add_peer() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper().await;
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
    cluster_actor.snapshot_topology().await.unwrap();

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
async fn test_requests_pending() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper().await;
    cluster_actor.block_write_reqs();

    //WHEN
    let (tx, rx) = tokio::sync::oneshot::channel();
    let write_r = WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None };
    let con_req = ConsensusRequest::new(write_r.clone(), tx, None);

    cluster_actor.req_consensus(con_req).await;

    // THEN
    assert!(timeout(Duration::from_millis(200), rx).await.is_err());
    assert_eq!(cluster_actor.pending_requests.as_ref().unwrap().len(), 1);
    assert_eq!(
        cluster_actor.pending_requests.as_mut().unwrap().pop_front().unwrap().request,
        write_r
    );
}

#[tokio::test]
async fn test_reconnection_on_gossip() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper().await;

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
        )])
        .await;

    assert!(handle.await.is_ok());
    assert!(rx.await.is_ok());
}

// ! When LazyOption is Lazy, rebalance request should not block
#[tokio::test]
async fn test_rebalance_request_with_lazy() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper().await;

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
    let mut cluster_actor = cluster_actor_create_helper().await;

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
    let mut cluster_actor = cluster_actor_create_helper().await;
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
    let mut cluster_actor = cluster_actor_create_helper().await;
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
    let mut cluster_actor = cluster_actor_create_helper().await;

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
    let mut cluster_actor = cluster_actor_create_helper().await;
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
    let mut cluster_actor = cluster_actor_create_helper().await;
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
    let mut cluster_actor = cluster_actor_create_helper().await;
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
