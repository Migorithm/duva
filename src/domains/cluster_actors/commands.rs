use crate::domains::append_only_files::log::LogIndex;
use crate::domains::peers::peer::Peer;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

use super::replication::{HeartBeatMessage, ReplicationInfo};

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo { leader_repl_id: String, commit_idx: u64 },
    SendHeartBeat,

    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus { log: WriteRequest, sender: tokio::sync::oneshot::Sender<Option<LogIndex>> },
    LeaderReceiveAcks(Vec<LogIndex>),
    SendCommitHeartBeat { offset: LogIndex },

    ReceiveHeartBeat(HeartBeatMessage),

    AcceptLeaderHeartBeat(HeartBeatMessage),
}

pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}
