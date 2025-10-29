mod cluster_managements;
mod elections;
mod partitionings;
mod replications;
#[allow(unused_variables)]
use super::actor::ClusterActorSender;
use super::*;
use crate::CacheManager;
use crate::Replication;
use crate::ReplicationId;
use crate::adapters::op_logs::memory_based::MemoryOpLogs;
use crate::domains::QueryIO;
use crate::domains::TSerdeDynamicRead;
use crate::domains::TSerdeDynamicWrite;
use crate::domains::caches::actor::CacheCommandSender;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::caches::command::CacheCommand;
use crate::presentation::clients::request::ClientReq;

use crate::domains::peers::command::HeartBeat;

use crate::domains::peers::connections::connection_types::ReadConnected;
use crate::domains::peers::connections::inbound::stream::InboundStream;

use crate::domains::peers::service::PeerListener;
use crate::types::Callback;
use crate::{
    domains::{IoError, TRead, TWrite},
    make_smart_pointer,
};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::mpsc::channel;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FakeReadWrite(Arc<Mutex<VecDeque<PeerMessage>>>);
make_smart_pointer!(FakeReadWrite, Arc<Mutex<VecDeque<PeerMessage>>>);

impl FakeReadWrite {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(VecDeque::new())))
    }
}
#[async_trait::async_trait]
impl TWrite for FakeReadWrite {
    async fn write(&mut self, io: QueryIO) -> Result<(), IoError> {
        panic!()
    }
}

#[async_trait::async_trait]
impl TSerdeDynamicRead for FakeReadWrite {
    async fn receive_peer_msgs(&mut self) -> Result<Vec<PeerMessage>, IoError> {
        let guard = self.0.lock().await;
        let values = guard.clone().drain(..).collect();
        Ok(values)
    }
}

#[async_trait::async_trait]
impl TSerdeDynamicWrite for FakeReadWrite {
    async fn send(&mut self, msg: PeerMessage) -> Result<(), IoError> {
        let mut guard = self.0.lock().await;
        guard.push_back(msg);
        Ok(())
    }
}

#[async_trait::async_trait]
impl TRead for FakeReadWrite {
    async fn read_values(&mut self) -> Result<Vec<QueryIO>, IoError> {
        panic!()
    }
}

pub(crate) struct Helper;
impl Helper {
    // Helper function to create cache manager with con_idx
    pub(crate) fn cache_manager() -> (Arc<AtomicU64>, CacheManager) {
        let con_idx = Arc::new(AtomicU64::new(0));
        let cache_manager = CacheManager::run_cache_actors(con_idx.clone());
        (con_idx, cache_manager)
    }

    pub(crate) async fn cache_manager_with_keys(
        keys: Vec<String>,
    ) -> (Arc<AtomicU64>, CacheManager) {
        let con_idx = Arc::new(AtomicU64::new(0));
        let cache_manager = CacheManager::run_cache_actors(con_idx.clone());
        for key in keys.clone() {
            cache_manager.route_set(CacheEntry::new(key, "value")).await.unwrap();
        }
        con_idx.store(keys.len() as u64, Ordering::Relaxed);
        (con_idx, cache_manager)
    }

    pub(crate) fn create_peer(
        cluster_sender: ClusterActorSender,
        con_idx: u64,
        repl_id: &ReplicationId,
        port: u16,

        role: ReplicationRole,
        fake_buf: FakeReadWrite,
        term: u64,
    ) -> (PeerIdentifier, Peer) {
        let key = PeerIdentifier::new("127.0.0.1", port);

        let kill_switch = PeerListener::spawn(fake_buf.clone(), cluster_sender, key.clone());
        let peer = Peer::new(
            fake_buf,
            {
                let id = key.clone();
                let replid = repl_id.clone();
                ReplicationState { node_id: id, last_log_index: con_idx, replid, role, term }
            },
            kill_switch,
        );
        (key, peer)
    }

    pub(crate) fn write(index_num: u64, term: u64, key: &str, value: &str) -> WriteOperation {
        WriteOperation {
            log_index: index_num,
            entry: LogEntry::Set { entry: CacheEntry::new(key, value) },
            term,
            session_req: None,
        }
    }
    pub(crate) fn session_write(
        index_num: u64,
        term: u64,
        key: &str,
        value: &str,
        session_req: ClientReq,
    ) -> WriteOperation {
        WriteOperation {
            log_index: index_num,
            entry: LogEntry::Set { entry: CacheEntry::new(key, value) },
            term,
            session_req: Some(session_req),
        }
    }

    fn heartbeat(
        term: u64,
        leader_commit_idx: Option<u64>,
        op_logs: Vec<WriteOperation>,
    ) -> HeartBeat {
        HeartBeat {
            term,
            leader_commit_idx,
            prev_log_index: if !op_logs.is_empty() { op_logs[0].log_index - 1 } else { 0 },
            prev_log_term: 0,
            append_entries: op_logs,
            banlist: vec![],
            from: PeerIdentifier::new("localhost", 8080),
            replid: ReplicationId::Key("localhost".to_string()),
            hop_count: 0,
            cluster_nodes: vec![],
            hashring: None,
        }
    }

    pub async fn cluster_actor(role: ReplicationRole) -> ClusterActor<MemoryOpLogs> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("duva.tp");

        let topology_writer =
            OpenOptions::new().create(true).write(true).truncate(true).open(path).unwrap();

        let state = ReplicationState {
            node_id: PeerIdentifier::new("127.0.0.1", 8080),
            replid: ReplicationId::Key("master".into()),
            role,
            last_log_index: 0,
            term: 0,
        };
        let replication = Replication::new(8080, MemoryOpLogs::default(), state);
        let (_, cache_manager) = Helper::cache_manager();
        ClusterActor::new(replication, 100, topology_writer, cache_manager)
    }

    fn cluster_member(
        actor: &mut ClusterActor<MemoryOpLogs>,
        fake_bufs: Vec<FakeReadWrite>,
        cluster_sender: ClusterActorSender,
        follower_con_idx: u64,
        replid: Option<ReplicationId>,
    ) {
        for fake_b in fake_bufs.into_iter() {
            let port = rand::random::<u16>();
            let key = PeerIdentifier::new("localhost", port);

            let kill_switch = PeerListener::spawn(
                ReadConnected(Box::new(fake_b.clone())),
                cluster_sender.clone(),
                key.clone(),
            );
            let term = actor.log_state().term;
            actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new(
                    fake_b.clone(),
                    {
                        let id = PeerIdentifier(format!("localhost:{port}"));
                        let replid = replid
                            .clone()
                            .unwrap_or_else(|| ReplicationId::Key("localhost".to_string()));
                        let role = ReplicationRole::Follower;
                        ReplicationState {
                            node_id: id,
                            last_log_index: follower_con_idx,
                            replid,
                            role,
                            term,
                        }
                    },
                    kill_switch,
                ),
            );
        }
    }

    fn consensus_request(
        callback: Callback<ConsensusClientResponse>,
        session_req: Option<ClientReq>,
    ) -> ConsensusRequest {
        ConsensusRequest {
            entry: LogEntry::Set { entry: CacheEntry::new("foo".to_string(), "bar") },
            callback,
            session_req,
        }
    }
}

// Helper function to setup blocked cluster actor with pending requests
pub(crate) async fn setup_blocked_cluster_actor_with_requests(
    num_requests: usize,
) -> ClusterActor<MemoryOpLogs> {
    let mut cluster_actor = Helper::cluster_actor(ReplicationRole::Leader).await;
    cluster_actor.block_write_reqs();

    for _ in 0..num_requests {
        let (callback, _rx) = Callback::create();
        cluster_actor
            .pending_reqs
            .as_mut()
            .unwrap()
            .add_req(Helper::consensus_request(callback, None));
    }

    cluster_actor
}

// Helper function to assert migration batch ack
pub(crate) async fn assert_expected_queryio(
    message_buf: &FakeReadWrite,
    expected_query_io: impl Into<PeerMessage>,
) {
    let mut sent_messages = message_buf.lock().await;

    let message = sent_messages.pop_front().unwrap();

    assert_eq!(message, expected_query_io.into());
}

#[cfg(test)]
impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(crate) fn test_add_peer(
        &mut self,
        port: u16,
        repl_id: Option<ReplicationId>,
        is_leader: bool,
    ) -> (FakeReadWrite, PeerIdentifier) {
        let buf = FakeReadWrite::new();
        let (id, peer) = Helper::create_peer(
            self.self_handler.clone(),
            0,
            &repl_id.unwrap_or_else(|| self.log_state().replid.clone()),
            port,
            if is_leader { ReplicationRole::Leader } else { ReplicationRole::Follower },
            buf.clone(),
            0,
        );

        self.members.insert(id.clone(), peer);
        (buf, id)
    }
}

#[tokio::test]
async fn test_hop_count_when_one() {
    // GIVEN
    let fanout = 2;

    // WHEN
    let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 1);
    // THEN
    assert_eq!(hop_count, 0);
}

#[tokio::test]
async fn test_hop_count_when_two() {
    // GIVEN
    let fanout = 2;

    // WHEN
    let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 2);
    // THEN
    assert_eq!(hop_count, 0);
}

#[tokio::test]
async fn test_hop_count_when_three() {
    // GIVEN
    let fanout = 2;

    // WHEN
    let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 3);
    // THEN
    assert_eq!(hop_count, 1);
}

#[tokio::test]
async fn test_hop_count_when_thirty() {
    // GIVEN
    let fanout = 2;

    // WHEN
    let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 30);
    // THEN
    assert_eq!(hop_count, 4);
}
