use crate::services::cluster::actors::PeerState;
use crate::services::cluster::peer::address::PeerAddrs;
use crate::services::cluster::peer::identifier::PeerIdentifier;
use crate::services::cluster::peer::kind::PeerKind;
use crate::services::cluster::replication::replication::Replication;
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
}

pub struct AddPeer {
    pub(crate) peer_addr: PeerIdentifier,
    pub(crate) stream: TcpStream,
    pub(crate) peer_kind: PeerKind,
}

