use crate::domains::QueryIO;
use crate::domains::cluster_actors::topology::Topology;
use crate::domains::operation_logs::LogEntry;
use crate::domains::peers::command::{BatchId, PeerCommand, PendingMigrationTask};
use crate::domains::peers::connections::connection_types::{ReadConnected, WriteConnected};
use crate::domains::peers::peer::Peer;
use crate::domains::replications::*;
use crate::prelude::PeerIdentifier;
use crate::types::{Callback, CallbackAwaiter};
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub enum ClusterCommand {
    ConnectionReq(ConnectionMessage),
    Scheduler(SchedulerMessage),
    Client(ClientMessage),
    Peer(PeerCommand),
}

#[derive(Debug, PartialEq, Eq)]
pub enum SchedulerMessage {
    SendPeriodicHeatBeat,
    SendAppendEntriesRPC,
    StartLeaderElection,
    RebalanceRequest { request_to: PeerIdentifier, lazy_option: LazyOption },
    ScheduleMigrationBatch(PendingMigrationTask, Callback<anyhow::Result<()>>),
    TryUnblockWriteReqs,
    SendBatchAck { batch_id: BatchId, to: PeerIdentifier },
}

impl From<SchedulerMessage> for ClusterCommand {
    fn from(msg: SchedulerMessage) -> Self {
        ClusterCommand::Scheduler(msg)
    }
}

#[derive(Debug, PartialEq)]
pub enum ConnectionMessage {
    ConnectToServer { connect_to: PeerIdentifier, callback: Callback<anyhow::Result<()>> },
    AcceptInboundPeer { read: ReadConnected, write: WriteConnected, host_ip: String },
    AddPeer(Peer, Option<Callback<anyhow::Result<()>>>),
    FollowerSetReplId(ReplicationId, PeerIdentifier),
    ActivateClusterSync(Callback<()>),
    RequestClusterSyncAwaiter(Callback<Option<CallbackAwaiter<()>>>),
}
impl From<ConnectionMessage> for ClusterCommand {
    fn from(msg: ConnectionMessage) -> Self {
        ClusterCommand::ConnectionReq(msg)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientMessage {
    GetPeers(Callback<Vec<PeerIdentifier>>),
    ReplicationState(Callback<ReplicationState>),
    Forget(PeerIdentifier, Callback<Option<()>>),
    ReplicaOf(PeerIdentifier, Callback<anyhow::Result<()>>),
    LeaderReqConsensus(ConsensusRequest),
    ClusterNodes(Callback<Vec<ReplicationState>>),
    GetRoles(Callback<Vec<(PeerIdentifier, ReplicationRole)>>),
    SubscribeToTopologyChange(Callback<tokio::sync::broadcast::Receiver<Topology>>),
    ClusterMeet(PeerIdentifier, LazyOption, Callback<anyhow::Result<()>>),
    GetTopology(Callback<Topology>),
    ClusterReshard(Callback<Result<(), anyhow::Error>>),
    CanEnter(Callback<()>),
}

impl From<ClientMessage> for ClusterCommand {
    fn from(msg: ClientMessage) -> Self {
        ClusterCommand::Client(msg)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ConsensusRequest {
    pub(crate) entry: LogEntry,
    pub(crate) callback: Callback<ConsensusClientResponse>,
    pub(crate) session_req: Option<SessionRequest>,
}

#[derive(Debug)]
pub(crate) enum ConsensusClientResponse {
    AlreadyProcessed { key: Vec<String> },
    Err(String),
    Result(anyhow::Result<QueryIO>),
}

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
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

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct SessionRequest {
    pub(crate) request_id: u64,
    pub(crate) client_id: String,
}
impl SessionRequest {
    pub(crate) fn new(request_id: u64, client_id: String) -> Self {
        Self { request_id, client_id }
    }
}
