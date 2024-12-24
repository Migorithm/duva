use tokio::net::TcpStream;
use crate::services::cluster::actor::PeerAddr;

pub enum ClusterCommand {
    AddPeer(PeerAddr, TcpStream),
    RemovePeer(PeerAddr),
    GetPeer(PeerAddr),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerAddr>>),
}