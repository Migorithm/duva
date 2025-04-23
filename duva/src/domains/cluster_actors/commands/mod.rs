mod election;
pub(crate) mod types;
mod write_con;
use super::{
    replication::{HeartBeatMessage, ReplicationId, ReplicationRole, ReplicationState},
    session::SessionRequest,
};
use crate::domains::{append_only_files::WriteOperation, peers::cluster_peer::ClusterNode};
pub(crate) use election::*;
use tokio::net::TcpStream;
pub(crate) use types::*;
pub(crate) use write_con::*;

use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

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
        log: WriteRequest,
        callback: tokio::sync::oneshot::Sender<ConsensusClientResponse>,
        session_req: Option<SessionRequest>,
    },
    ReplicationResponse(ReplicationResponse),
    SendCommitHeartBeat {
        log_idx: u64,
    },
    AppendEntriesRPC(HeartBeatMessage),

    SendAppendEntriesRPC,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<ClusterNode>>),
    StartLeaderElection,
    VoteElection(RequestVote),
    ApplyElectionVote(RequestVoteReply),
    ClusterHeartBeat(HeartBeatMessage),
    GetRole(tokio::sync::oneshot::Sender<ReplicationRole>),
    SubscribeToTopologyChange(
        tokio::sync::oneshot::Sender<tokio::sync::broadcast::Receiver<Vec<PeerIdentifier>>>,
    ),
}
