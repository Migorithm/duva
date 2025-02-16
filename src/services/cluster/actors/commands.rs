use crate::services::aof::{WriteOperation, WriteRequest};
use crate::services::cluster::peers::address::PeerAddrs;
use crate::services::cluster::peers::identifier::PeerIdentifier;

use crate::services::cluster::peers::peer::Peer;
use crate::services::cluster::replications::replication::{HeartBeatMessage, ReplicationInfo};

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<PeerAddrs>),
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
