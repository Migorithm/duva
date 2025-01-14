use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;

use super::actor::PeerAddr;

pub enum ClusterCommand {
    AddPeer { peer_addr: PeerAddr, stream: TcpStream, peer_kind: PeerKind },
    RemovePeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerAddr>>),
    Write(ClusterWriteCommand),
}

impl ClusterCommand {
    pub(crate) fn ping() -> Self {
        Self::Write(ClusterWriteCommand::Ping)
    }
}

pub enum ClusterWriteCommand {
    Replicate { query: QueryIO, peer_addr: PeerAddr },
    Ping,
}

#[derive(Clone)]
pub enum PeerKind {
    Peer,
    Replica,
    Master,
}

impl PeerKind {
    pub fn peer_kind(self_repl_id: &str, other_repl_id: &str) -> Self {
        if self_repl_id == "?" || self_repl_id == other_repl_id {
            Self::Replica
        } else {
            Self::Peer
        }
    }
}
