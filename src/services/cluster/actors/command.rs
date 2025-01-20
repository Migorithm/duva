use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;

use super::{
    replication::Replication,
    types::{PeerAddr, PeerAddrs, PeerKind},
};

pub enum ClusterCommand {
    AddPeer { peer_addr: PeerAddr, stream: TcpStream, peer_kind: PeerKind },
    RemovePeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<PeerAddrs>),
    ReplicationInfo(tokio::sync::oneshot::Sender<Replication>),
    SetReplicationId(String),
    Write(ClusterWriteCommand),
}

impl ClusterCommand {
    pub(crate) fn ping() -> Self {
        Self::Write(ClusterWriteCommand::Ping)
    }
}

pub enum ClusterWriteCommand {
    Replicate { query: QueryIO },
    Ping,
}

#[derive(Debug)]
pub enum MasterCommand {
    Ping,
    Replicate { query: QueryIO },
    Sync(QueryIO),
}
pub enum SlaveCommand {
    Ping,
}

pub enum PeerCommand {
    Ping,
}

impl TryFrom<QueryIO> for MasterCommand {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::File(v) => Ok(Self::Sync(v.into())),
            QueryIO::SimpleString(s) => match s.to_lowercase().as_str() {
                "ping" => Ok(Self::Ping),
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
}

impl TryFrom<QueryIO> for SlaveCommand {
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
