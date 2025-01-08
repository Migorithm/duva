use crate::services::cluster::actor::PeerAddr;
use tokio::net::TcpStream;

pub enum ClusterCommand {
    AddPeer {
        peer_addr: PeerAddr,
        stream: TcpStream,
        peer_kind: PeerKind,
    },
    RemovePeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerAddr>>),
}

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