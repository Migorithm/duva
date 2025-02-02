use crate::services::cluster::actors::{
    replication::Replication,
    PeerState,
};
use crate::services::cluster::peer::identifier::{PeerAddrs, PeerIdentifier};
use crate::services::cluster::peer::kind::PeerKind;
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

#[derive(Debug)]
pub enum CommandFromMaster {
    HeartBeat(PeerState),
    Replicate { query: QueryIO },
    Sync(QueryIO),
}
pub enum CommandFromSlave {
    HeartBeat(PeerState),
}

impl TryFrom<QueryIO> for CommandFromMaster {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            file @ QueryIO::File(_) => Ok(Self::Sync(file)),
            QueryIO::PeerState(peer_state) => Ok(Self::HeartBeat(peer_state)),
            _ => todo!(),
        }
    }
}

impl TryFrom<QueryIO> for CommandFromSlave {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::PeerState(peer_state) => Ok(CommandFromSlave::HeartBeat(peer_state)),
            _ => todo!(),
        }
    }
}
