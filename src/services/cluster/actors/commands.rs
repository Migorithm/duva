use crate::services::aof::{WriteOperation, WriteRequest};

use crate::services::cluster::peers::identifier::PeerIdentifier;

use crate::services::cluster::actors::replication::{HeartBeatMessage, ReplicationInfo};
use crate::services::cluster::peers::peer::Peer;

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo { master_repl_id: String, offset: u64 },
    SendHeartBeat,

    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus { log: WriteRequest, sender: tokio::sync::oneshot::Sender<Option<u64>> },
    LeaderReceiveAcks(Vec<u64>),

    // peer listener commands
    ReceiveHeartBeat(HeartBeatMessage),
    FollowerReceiveLogEntries(Vec<WriteOperation>),
}

pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}
