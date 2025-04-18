mod election;
pub(crate) mod types;
mod write_con;
use super::{
    replication::{HeartBeatMessage, ReplicationId, ReplicationState, ReplicationRole},
    session::SessionRequest,
};
use crate::domains::append_only_files::WriteOperation;
pub(crate) use election::*;
pub(crate) use types::*;
pub(crate) use write_con::*;

use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

#[derive(Debug)]
pub(crate) enum ClusterCommand {
    AddPeer(AddPeer, tokio::sync::oneshot::Sender<()>),
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
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
    FetchCurrentState(tokio::sync::oneshot::Sender<SyncLogs>),
    StartLeaderElection,
    VoteElection(RequestVote),
    ApplyElectionVote(RequestVoteReply),
    ClusterHeartBeat(HeartBeatMessage),
    GetRole(tokio::sync::oneshot::Sender<ReplicationRole>),
    SubscribeToTopologyChange(
        tokio::sync::oneshot::Sender<tokio::sync::broadcast::Receiver<Vec<PeerIdentifier>>>,
    ),
}
