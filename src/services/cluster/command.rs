use crate::services::cluster::actor::PeerAddr;
use tokio::net::TcpStream;

pub enum ClusterCommand {
    AddPeer {
        peer_addr: PeerAddr,
        stream: TcpStream,
        is_slave: bool,
    },
    RemovePeer(PeerAddr),
    GetPeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerAddr>>),
}
