use crate::services::cluster::actor::PeerAddr;
use tokio::net::TcpStream;

pub enum ClusterCommand {
    AddPeer(PeerAddr, TcpStream),
    RemovePeer(PeerAddr),
    GetPeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerAddr>>),
}
