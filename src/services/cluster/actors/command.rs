use super::{
    replication::Replication,
    types::{PeerAddr, PeerAddrs, PeerKind},
};
use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;

pub enum ClusterCommand {
    AddPeer { peer_addr: PeerAddr, stream: TcpStream, peer_kind: PeerKind },
    RemovePeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<PeerAddrs>),
    ReplicationInfo(tokio::sync::oneshot::Sender<Replication>),
    SetReplicationId(String),
    Ping,
    Replicate { query: QueryIO },
}

#[derive(Debug)]
pub enum CommandFromMaster {
    Ping,
    Replicate { query: QueryIO },
    Sync(QueryIO),
}
pub enum CommandFromSlave {
    Ping,
}

impl TryFrom<QueryIO> for CommandFromMaster {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            file @ QueryIO::File(_) => Ok(Self::Sync(file)),
            QueryIO::SimpleString(s) => match s.to_lowercase().as_str() {
                "ping" => Ok(Self::Ping),
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}

impl TryFrom<QueryIO> for CommandFromSlave {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::SimpleString(s) => match s.to_lowercase().as_str() {
                "ping" => Ok(Self::Ping),

                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}
