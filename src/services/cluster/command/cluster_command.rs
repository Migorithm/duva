use crate::services::cluster::peers::address::PeerAddrs;
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::replications::replication::{PeerState, Replication};
use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;

pub enum ClusterCommand {
    AddPeer(AddPeer),
    RemovePeer(PeerIdentifier),
    GetPeers(tokio::sync::oneshot::Sender<PeerAddrs>),
    ReplicationInfo(tokio::sync::oneshot::Sender<Replication>),
    SetReplicationInfo { master_repl_id: String, offset: u64 },
    SendHeartBeat,
    Replicate { query: QueryIO },
    ReportAlive { state: PeerState },
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LiftBan(PeerIdentifier),
}

pub struct AddPeer {
    pub(crate) peer_addr: PeerIdentifier,
    pub(crate) stream: TcpStream,
    pub(crate) peer_kind: PeerKind,
}
