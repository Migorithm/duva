use tokio::net::TcpStream;

use crate::{
    ReplicationState,
    domains::{
        cluster_actors::{
            replication::{HeartBeatMessage, ReplicationId, ReplicationRole},
            session::SessionRequest,
        },
        operation_logs::WriteRequest,
        peers::peer::{Peer, PeerState},
    },
    prelude::PeerIdentifier,
};

use super::{ConsensusClientResponse, ReplicationResponse, RequestVote, RequestVoteReply};

#[derive(Debug)]
pub(crate) enum ClusterCommand {
    DiscoverCluster {
        connect_to: PeerIdentifier,
        callback: tokio::sync::oneshot::Sender<()>,
    },
    AcceptPeer {
        stream: TcpStream,
    },

    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationState>),

    SendClusterHeatBeat,
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    ReplicaOf(PeerIdentifier, tokio::sync::oneshot::Sender<anyhow::Result<()>>),
    LeaderReqConsensus(ConsensusRequest),
    ReplicationResponse(ReplicationResponse),
    AppendEntriesRPC(HeartBeatMessage),

    SendAppendEntriesRPC,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<PeerState>>),
    StartLeaderElection,
    VoteElection(RequestVote),
    ApplyElectionVote(RequestVoteReply),
    ClusterHeartBeat(HeartBeatMessage),
    GetRole(tokio::sync::oneshot::Sender<ReplicationRole>),
    SubscribeToTopologyChange(
        tokio::sync::oneshot::Sender<tokio::sync::broadcast::Receiver<Vec<PeerIdentifier>>>,
    ),
    StoreSnapshotMetadata {
        replid: ReplicationId,
        hwm: u64,
    },
    ClusterMeet(PeerIdentifier, tokio::sync::oneshot::Sender<anyhow::Result<()>>),
    AddPeer(Peer),
    FollowerSetReplId(ReplicationId),
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

impl From<ConsensusRequest> for ClusterCommand {
    fn from(request: ConsensusRequest) -> Self {
        Self::LeaderReqConsensus(request)
    }
}
