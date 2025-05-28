mod cluster_managements;
mod elections;
mod partitionings;
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
use crate::make_smart_pointer;
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

fn add_replica_helper(actor: &mut ClusterActor<MemoryOpLogs>, port: u16) -> FakeReadWrite {
    let buf = FakeReadWrite::new();
    let (id, peer) = create_peer_helper(
        actor.self_handler.clone(),
        0,
        &actor.replication.replid,
        port,
        NodeKind::Replica,
        buf.clone(),
    );

    actor.members.insert(id, peer);
    buf
}
pub async fn cluster_actor_create_helper(role: ReplicationRole) -> ClusterActor<MemoryOpLogs> {
    let replication =
        ReplicationState::new(ReplicationId::Key("master".into()), role, "localhost", 8080, 0);
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("duva.tp");

    let topology_writer =
        OpenOptions::new().create(true).write(true).truncate(true).open(path).await.unwrap();

    ClusterActor::new(100, replication, 100, topology_writer, MemoryOpLogs::default())
}

#[derive(Debug, Clone)]
struct FakeReadWrite(Arc<Mutex<VecDeque<QueryIO>>>);
make_smart_pointer!(FakeReadWrite, Arc<Mutex<VecDeque<QueryIO>>>);

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
    async fn read_bytes(&mut self, _buf: &mut BytesMut) -> Result<(), IoError> {
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
async fn test_requests_pending() {
    // GIVEN
    let mut cluster_actor = cluster_actor_create_helper(ReplicationRole::Leader).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let write_r = WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None };
    let con_req = ConsensusRequest::new(write_r.clone(), tx, None);

    //WHEN
    cluster_actor.test_block_write_reqs();
    cluster_actor.req_consensus(con_req).await;

    // THEN
    assert!(timeout(Duration::from_millis(200), rx).await.is_err());
    assert_eq!(cluster_actor.pending_requests.as_ref().unwrap().len(), 1);
    assert_eq!(
        cluster_actor.pending_requests.as_mut().unwrap().pop_front().unwrap().request,
        write_r
    );
}
