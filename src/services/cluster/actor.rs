use crate::make_smart_pointer;
use crate::services::cluster::command::{ClusterCommand, PeerKind};
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use std::collections::{BTreeMap, HashSet};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Default)]
pub struct ClusterActor {
    // TODO change PeerAddr to PeerIdentifier
    pub peers: HashSet<PeerAddr>,
    pub write_members: BTreeMap<PeerAddr, ClusterWriteConnected>,
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);

impl ClusterActor {
    async fn heartbeat(&mut self) {
        for connected in self.write_members.values_mut() {
            let _ = connected.ping().await;
        }
    }

    // * Add peer to the cluster
    // * This function is called when a new peer is connected to peer listener
    // * Some are replicas and some are cluster members
    pub async fn add_peer(
        &mut self,
        peer_addr: PeerAddr,
        read_half: OwnedReadHalf,
        peer_kind: PeerKind,

        read_sender: Sender<ClusterReadCommand>,
    ) {
        let _ = read_sender
            .send(ClusterReadCommand::Add {
                addr: peer_addr.clone(),
                buffer: read_half,
            })
            .await;
    }

    pub fn remove_peer(&mut self, peer_addr: PeerAddr) {
        self.peers.retain(|addr| addr != &peer_addr);
    }

    pub async fn handle(mut self, mut recv: Receiver<ClusterCommand>) {
        let (read_sender, lr) = tokio::sync::mpsc::channel::<ClusterReadCommand>(1000);

        tokio::spawn(run_cluster_read_actor(lr));

        while let Some(command) = recv.recv().await {
            match command {
                ClusterCommand::AddPeer {
                    peer_addr,
                    stream,
                    peer_kind,
                } => {
                    let (r, w) = stream.into_split();
                    self.add_peer(peer_addr.clone(), r, peer_kind.clone(), read_sender.clone())
                        .await;

                    self.write_members
                        .entry(peer_addr)
                        .or_insert(ClusterWriteConnected::new(w, peer_kind));
                }
                ClusterCommand::RemovePeer(peer_addr) => {
                    self.remove_peer(peer_addr);
                }

                ClusterCommand::GetPeers(callback) => {
                    // send
                    let _ = callback.send(self.peers.iter().cloned().collect());
                }
                ClusterCommand::Wirte(write_cmd) => match write_cmd {
                    ClusterWriteCommand::Replicate { query } => todo!(),
                    ClusterWriteCommand::Ping => {
                        self.heartbeat().await;
                    }
                },
            }
        }
    }
}

async fn run_cluster_read_actor(mut sr: Receiver<ClusterReadCommand>) {
    let mut members = BTreeMap::new();

    while let Some(command) = sr.recv().await {
        match command {
            // TODO PING(heartbeat) must come with some metadata to identify the sender
            ClusterReadCommand::Ping => {
                // do something - failure detection!
            }
            ClusterReadCommand::Add { addr, buffer } => {
                members.entry(addr).or_insert(buffer);
            }
        }
    }
}

struct ClusterWriteActor {
    members: BTreeMap<PeerAddr, ClusterWriteConnected>,
}
impl ClusterWriteActor {
    fn new() -> Self {
        Self {
            members: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub enum ClusterWriteConnected {
    Replica { stream: OwnedWriteHalf },
    Peer { stream: OwnedWriteHalf },
    Master { stream: OwnedWriteHalf },
}
impl ClusterWriteConnected {
    pub fn new(stream: OwnedWriteHalf, peer_kind: PeerKind) -> Self {
        match peer_kind {
            PeerKind::Peer => Self::Peer { stream },
            PeerKind::Replica => Self::Replica { stream },
            PeerKind::Master => Self::Master { stream },
        }
    }
    async fn ping(&mut self) {
        let msg = QueryIO::SimpleString("PING".to_string()).serialize();

        match self {
            ClusterWriteConnected::Replica { stream } => {
                let _ = stream.write(msg.as_bytes()).await;
            }
            ClusterWriteConnected::Peer { stream } => {
                let _ = stream.write(msg.as_bytes()).await;
            }
            ClusterWriteConnected::Master { stream } => {
                let _ = stream.write(msg.as_bytes()).await;
            }
        }
    }
}

pub enum ClusterWriteCommand {
    Replicate { query: QueryIO },
    Ping,
}

pub enum ClusterReadCommand {
    Ping,
    Add {
        addr: PeerAddr,
        buffer: OwnedReadHalf,
    },
}
