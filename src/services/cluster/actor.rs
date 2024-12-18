use std::collections::BTreeMap;

use tokio::sync::mpsc::Receiver;

use crate::services::stream_manager::interface::TStream;

pub enum ClusterCommand<T: TStream> {
    AddPeer(PeerAddr, T),
    RemovePeer(PeerAddr),
    GetPeer(PeerAddr),
}

pub struct ClusterActor<T: TStream> {
    peers: BTreeMap<PeerAddr, Connected<T>>,
}
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

impl<T: TStream> ClusterActor<T> {
    pub fn new() -> Self {
        Self {
            peers: BTreeMap::new(),
        }
    }
    // * Add peer to the cluster
    // * This function is called when a new peer is connected to peer listener
    // * Some are replicas and some are cluster members
    pub fn add_peer(&mut self, peer_addr: PeerAddr, stream: T) {}

    pub fn remove_peer(&mut self, peer_addr: PeerAddr) {
        self.peers.remove(&peer_addr);
    }

    pub fn get_peer(&self, peer_addr: &PeerAddr) -> Option<&Connected<T>> {
        self.peers.get(peer_addr)
    }
    pub async fn handle(mut self, mut recv: Receiver<ClusterCommand<T>>) {
        while let Some(command) = recv.recv().await {
            match command {
                ClusterCommand::AddPeer(peer_addr, connected) => {
                    self.add_peer(peer_addr, connected);
                }
                ClusterCommand::RemovePeer(peer_addr) => {
                    self.remove_peer(peer_addr);
                }
                ClusterCommand::GetPeer(peer_addr) => {
                    self.get_peer(&peer_addr);
                }
            }
        }
    }
}
