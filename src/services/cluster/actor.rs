use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::interval;
use tokio::{net::TcpStream, sync::mpsc::Receiver};

use crate::make_smart_pointer;
use crate::services::cluster::command::ClusterCommand;
use crate::services::stream_manager::interface::TStream;
use crate::services::stream_manager::query_io::QueryIO;

pub struct ClusterActor {
    pub peers: BTreeMap<PeerAddr, Connected>,
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);

#[derive(Debug)]
pub enum Connected {
    Replica { stream: TcpStream },
    ClusterMember { stream: TcpStream },
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
    pub fn add_peer(&mut self, peer_addr: PeerAddr, stream: TcpStream, is_slave: bool) {}

    pub fn remove_peer(&mut self, peer_addr: PeerAddr) {
        self.peers.remove(&peer_addr);
    }

    pub fn get_peer(&self, peer_addr: &PeerAddr) -> Option<&Connected> {
        self.peers.get(peer_addr)
    }
    pub async fn handle(mut self, mut recv: Receiver<ClusterCommand>) {
        while let Some(command) = recv.recv().await {
            match command {
                ClusterCommand::AddPeer {
                    peer_addr,
                    stream,
                    is_slave,
                } => {
                    self.add_peer(peer_addr, stream, is_slave);
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

    // read half and write half should be considered?
    // for write half, how do we have replication operation inturrupt other write operations like heartbeat?
    // Potentially, we may want to clone/copy and synchronize when cluster information is updated?
    async fn schedule_heartbeat(&mut self) -> anyhow::Result<()> {
        // 1000 mills just because that's default for Redis.
        const HEARTBEAT_INTERVAL: u64 = 1000;

        let mut interval = interval(Duration::from_millis(HEARTBEAT_INTERVAL));

        loop {
            interval.tick().await;

            for (peer_addr, connected) in self.peers.iter_mut() {
                match connected {
                    Connected::Replica { stream } | Connected::ClusterMember { stream } => {
                        stream
                            .write(QueryIO::SimpleString("PING".to_string()))
                            .await
                            .unwrap();
                    }

                    Connected::None => todo!(),
                }
            }
        }

        Ok(())
    }
}
