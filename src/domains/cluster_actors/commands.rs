use super::replication::{HeartBeatMessage, ReplicationInfo};
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::peers::peer::Peer;
use crate::domains::saves::snapshot::snapshot::Snapshot;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo { leader_repl_id: PeerIdentifier, hwm: u64 },
    ApplySnapshot(Snapshot),
    SendHeartBeat,

    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus { log: WriteRequest, sender: tokio::sync::oneshot::Sender<Option<LogIndex>> },
    LeaderReceiveAcks(Vec<LogIndex>),
    SendCommitHeartBeat { log_idx: LogIndex },

    ReceiveHeartBeat(HeartBeatMessage),

    HandleLeaderHeartBeat(HeartBeatMessage),
    SendLeaderHeartBeat,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
    FetchCurrentState(tokio::sync::oneshot::Sender<Vec<WriteOperation>>),
}

pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}
