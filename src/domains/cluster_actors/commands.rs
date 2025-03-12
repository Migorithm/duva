use super::replication::{HeartBeatMessage, ReplicationInfo};
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::peers::peer::Peer;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo {
        leader_repl_id: PeerIdentifier,
        hwm: u64,
    },
    InstallLeaderState(Vec<WriteOperation>),
    SendHeartBeat,

    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus {
        log: WriteRequest,
        sender: tokio::sync::oneshot::Sender<WriteConsensusResponse>,
    },
    LeaderReceiveAcks(Vec<LogIndex>),
    SendCommitHeartBeat {
        log_idx: LogIndex,
    },

    ReceiveHeartBeat(HeartBeatMessage),

    HandleLeaderHeartBeat(HeartBeatMessage),
    SendLeaderHeartBeat,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
    FetchCurrentState(tokio::sync::oneshot::Sender<Vec<WriteOperation>>),
    StartLeaderElection(tokio::sync::oneshot::Sender<()>),
}

#[derive(Debug)]
pub enum WriteConsensusResponse {
    LogIndex(Option<LogIndex>),

    Err(String),
}

pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}
