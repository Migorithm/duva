use super::{
    replication::Replication,
    types::{PeerAddr, PeerAddrs, PeerKind},
    PeerState,
};
use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;

pub enum ClusterCommand {
    AddPeer(AddPeer),
    RemovePeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<PeerAddrs>),
    ReplicationInfo(tokio::sync::oneshot::Sender<Replication>),
    SetReplicationInfo { master_repl_id: String, offset: u64 },
    SendHeartBeat,
    Replicate { query: QueryIO },
}

pub struct AddPeer {
    pub(crate) peer_addr: PeerAddr,
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
