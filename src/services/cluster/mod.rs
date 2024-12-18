pub mod actor;
use actor::ClusterActor;
use tokio::sync::mpsc::Sender;

use super::stream_manager::interface::TStream;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PeerAddr(pub String);
pub enum Connected<T: TStream> {
    Replica {
        peer_stream: T,
        replication_stream: T,
    },
    ClusterMemeber {
        peer_stream: T,
    },
}

pub enum ClusterCommand<T: TStream> {
    AddPeer(PeerAddr, Connected<T>),
    RemovePeer(PeerAddr),
    GetPeer(PeerAddr),
}

pub struct ClusterManager<T: TStream>(Sender<ClusterCommand<T>>);

impl<T: TStream> ClusterManager<T> {
    pub fn run() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let cluster_manager = ClusterActor::new();
        tokio::spawn(cluster_manager.handle(rx));
        Self(tx)
    }
}
