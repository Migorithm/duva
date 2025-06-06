use crate::ReplicationState;
use crate::domains::cluster_actors::hash_ring::MigrationTarget;
use crate::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use crate::domains::operation_logs::WriteRequest;
use crate::domains::peers::command::PeerCommand;
use crate::domains::peers::peer::{Peer, PeerState};
use crate::prelude::PeerIdentifier;
use std::str::FromStr;
use tokio::net::TcpStream;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum ClusterCommand {
    ConnectionReq(ConnectionMessage),
    Scheduler(SchedulerMessage),
    Client(ClientMessage),
    Peer(PeerCommand),
}

#[derive(Debug)]
pub enum SchedulerMessage {
    SendPeriodicHeatBeat,
    SendAppendEntriesRPC,
    StartLeaderElection,
    RebalanceRequest { request_to: PeerIdentifier, lazy_option: LazyOption },
    ScheduleMigrationBatch(MigrationTarget, tokio::sync::oneshot::Sender<anyhow::Result<()>>),
}
impl From<SchedulerMessage> for ClusterCommand {
    fn from(msg: SchedulerMessage) -> Self {
        ClusterCommand::Scheduler(msg)
    }
}

#[derive(Debug)]
pub enum ConnectionMessage {
    ConnectToServer {
        connect_to: PeerIdentifier,
        callback: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    },
    AcceptInboundPeer {
        stream: TcpStream,
    },

    AddPeer(Peer, Option<tokio::sync::oneshot::Sender<anyhow::Result<()>>>),
    FollowerSetReplId(ReplicationId),
}
impl From<ConnectionMessage> for ClusterCommand {
    fn from(msg: ConnectionMessage) -> Self {
        ClusterCommand::ConnectionReq(msg)
    }
}

#[derive(Debug)]
pub enum ClientMessage {
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationState>),
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    ReplicaOf(PeerIdentifier, tokio::sync::oneshot::Sender<anyhow::Result<()>>),
    LeaderReqConsensus(ConsensusRequest),
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<PeerState>>),
    GetRole(tokio::sync::oneshot::Sender<ReplicationRole>),
    SubscribeToTopologyChange(
        tokio::sync::oneshot::Sender<tokio::sync::broadcast::Receiver<Vec<PeerIdentifier>>>,
    ),
    ClusterMeet(PeerIdentifier, LazyOption, tokio::sync::oneshot::Sender<anyhow::Result<()>>),
}

impl From<ClientMessage> for ClusterCommand {
    fn from(msg: ClientMessage) -> Self {
        ClusterCommand::Client(msg)
    }
}

#[derive(Debug)]
pub(crate) struct ConsensusRequest {
    pub(crate) request: WriteRequest,
    pub(crate) callback: tokio::sync::oneshot::Sender<anyhow::Result<ConsensusClientResponse>>,
    pub(crate) session_req: Option<SessionRequest>,
}
impl ConsensusRequest {
    pub(crate) fn new(
        request: WriteRequest,
        callback: tokio::sync::oneshot::Sender<anyhow::Result<ConsensusClientResponse>>,
        session_req: Option<SessionRequest>,
    ) -> Self {
        Self { request, callback, session_req }
    }
}

#[derive(Debug)]
pub(crate) enum ConsensusClientResponse {
    AlreadyProcessed { key: String, index: u64 },
    LogIndex(Option<u64>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LazyOption {
    Lazy,
    Eager,
}

impl FromStr for LazyOption {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            | "lazy" => Ok(LazyOption::Lazy),
            | "eager" => Ok(LazyOption::Eager),
            | _ => Err(anyhow::anyhow!("Invalid value for LazyOption")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRequest {
    pub(crate) request_id: u64,
    pub(crate) client_id: Uuid,
}
impl SessionRequest {
    pub(crate) fn new(request_id: u64, client_id: Uuid) -> Self {
        Self { request_id, client_id }
    }
}
