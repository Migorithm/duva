use crate::domains::append_only_files::log::LogIndex;
use crate::domains::peers::peer::Peer;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

use super::replication::{HeartBeatMessage, ReplicationInfo};

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo { leader_repl_id: PeerIdentifier, hwm: u64 },
    SendHeartBeat,

    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus { log: WriteRequest, sender: tokio::sync::oneshot::Sender<Option<LogIndex>> },
    LeaderReceiveAcks(Vec<LogIndex>),
    SendCommitHeartBeat { log_idx: LogIndex },

    ReceiveHeartBeat(HeartBeatMessage),

    HandleLeaderHeartBeat(HeartBeatMessage),
    SendLeaderHeartBeat,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
}

pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}
