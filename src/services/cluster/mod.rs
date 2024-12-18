use tokio::sync::mpsc::{Receiver, Sender};

use super::stream_manager::interface::TStream;
use std::collections::BTreeMap;

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

pub struct ClusterManager<T: TStream> {
    peers: BTreeMap<PeerAddr, Connected<T>>,
}

impl<T: TStream> ClusterManager<T> {
    pub fn new() -> Self {
        Self {
            peers: BTreeMap::new(),
        }
    }
    pub fn add_peer(&mut self, peer_addr: PeerAddr, connected: Connected<T>) {
        self.peers.insert(peer_addr, connected);
    }

    pub fn remove_peer(&mut self, peer_addr: PeerAddr) {
        self.peers.remove(&peer_addr);
    }

    pub fn get_peer(&self, peer_addr: &PeerAddr) -> Option<&Connected<T>> {
        self.peers.get(peer_addr)
    }

    pub fn run() -> Sender<ClusterCommand<T>> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let cluster_manager = ClusterManager::new();
        tokio::spawn(cluster_manager.handle(rx));
        tx
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
