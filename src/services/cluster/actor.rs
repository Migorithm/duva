use std::collections::BTreeMap;

use tokio::sync::mpsc::Receiver;

use crate::services::stream_manager::interface::TStream;

use super::{ClusterCommand, Connected, PeerAddr};

pub struct ClusterActor<T: TStream> {
    peers: BTreeMap<PeerAddr, Connected<T>>,
}

impl<T: TStream> ClusterActor<T> {
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
