use tokio::net::TcpStream;

use crate::{
    ReplicationState,
    domains::{
        cluster_actors::{
            replication::{HeartBeatMessage, ReplicationId, ReplicationRole},
            session::SessionRequest,
        },
        operation_logs::{WriteOperation, WriteRequest},
        peers::peer::PeerState,
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

    SetReplicationInfo {
        replid: ReplicationId,
        hwm: u64,
    },
    InstallLeaderState(Vec<WriteOperation>),
    SendClusterHeatBeat,
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    ReplicaOf(PeerIdentifier, tokio::sync::oneshot::Sender<()>),
    LeaderReqConsensus {
        request: WriteRequest,
        callback: tokio::sync::oneshot::Sender<anyhow::Result<ConsensusClientResponse>>,
        session_req: Option<SessionRequest>,
    },
    ReplicationResponse(ReplicationResponse),
    SendCommitHeartBeat {
        log_idx: u64,
    },
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
}
