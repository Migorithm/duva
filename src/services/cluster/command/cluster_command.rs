use crate::services::aof::WriteRequest;
use crate::services::cluster::peers::address::PeerAddrs;
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::peers::peer::Peer;
use crate::services::cluster::replications::replication::{HeartBeatMessage, ReplicationInfo};
use crate::services::query_io::QueryIO;

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<PeerAddrs>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo { master_repl_id: String, offset: u64 },
    SendHeartBeat,
    Replicate { query: QueryIO },
    ReceiveHeartBeat(HeartBeatMessage),
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    Consensus { log: WriteRequest, sender: tokio::sync::oneshot::Sender<u64> },
    CommitLog(u64),
}

pub struct AddPeer {
    pub(crate) peer_addr: PeerIdentifier,
    pub(crate) peer: Peer,
}
