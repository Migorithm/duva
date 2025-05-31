mod cluster_managements;
mod elections;
mod partitionings;
mod replications;
#[allow(unused_variables)]
use super::actor::ClusterCommandHandler;
use super::actor::cluster_actor_setups::FakeReadWrite;
use super::session::SessionRequest;
use super::*;
use crate::CacheManager;
use crate::NodeKind;
use crate::ReplicationId;
use crate::ReplicationState;
use crate::adapters::op_logs::memory_based::MemoryOpLogs;
use crate::domains::QueryIO;
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
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs::OpenOptions;
use tokio::net::TcpListener;

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

pub async fn cluster_actor_create_helper(role: ReplicationRole) -> ClusterActor<MemoryOpLogs> {
    let replication =
        ReplicationState::new(ReplicationId::Key("master".into()), role, "localhost", 8080, 0);
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("duva.tp");

    let topology_writer =
        OpenOptions::new().create(true).write(true).truncate(true).open(path).await.unwrap();

    ClusterActor::new(100, replication, 100, topology_writer, MemoryOpLogs::default())
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
