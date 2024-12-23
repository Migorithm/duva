use std::collections::BTreeMap;

use tokio::{net::TcpStream, sync::mpsc::Receiver};

use crate::make_smart_pointer;
use crate::services::cluster::command::ClusterCommand;

pub struct ClusterActor {
    pub peers: BTreeMap<PeerAddr, Connected>,
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);

#[derive(Debug)]
pub enum Connected {
    Replica {
        peer_stream: TcpStream,
        replication_stream: TcpStream,
    },
    ClusterMember {
        peer_stream: TcpStream,
    },
    None,
}

impl ClusterActor {
    pub fn new() -> Self {
        Self {
            peers: BTreeMap::new(),
        }
    }

    // * Add peer to the cluster
    // * This function is called when a new peer is connected to peer listener
    // * Some are replicas and some are cluster members
    pub fn add_peer(&mut self, peer_addr: PeerAddr, stream: TcpStream) {}

    pub fn remove_peer(&mut self, peer_addr: PeerAddr) {
        self.peers.remove(&peer_addr);
    }

    pub fn get_peer(&self, peer_addr: &PeerAddr) -> Option<&Connected> {
        self.peers.get(peer_addr)
    }
    pub async fn handle(mut self, mut recv: Receiver<ClusterCommand>) {
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
                ClusterCommand::GetPeers(callback) => {
                    // send
                    callback.send(self.peers.keys().cloned().collect());
                }
            }
        }
    }
}
