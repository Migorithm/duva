use crate::make_smart_pointer;
use crate::services::cluster::command::ClusterCommand;
use crate::services::connection_manager::interface::TWrite;
use crate::services::connection_manager::query_io::QueryIO;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::Sender;
use tokio::time::interval;
use tokio::{net::TcpStream, sync::mpsc::Receiver};

pub struct ClusterActor {
    // TODO change PeerAddr to PeerIdentifier
    pub peers: HashSet<PeerAddr>,
}
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);

impl ClusterActor {
    pub fn new() -> Self {
        Self {
            peers: HashSet::new(),
        }
    }

    // * Add peer to the cluster
    // * This function is called when a new peer is connected to peer listener
    // * Some are replicas and some are cluster members
    pub async fn add_peer(
        &mut self,
        peer_addr: PeerAddr,
        stream: TcpStream,
        is_slave: bool,
        write_sender: Sender<ClusterWriteCommand>,
        read_sender: Sender<ClusterReadCommand>,
    ) {
        let (r, w) = stream.into_split();
        let _ = write_sender
            .send(ClusterWriteCommand::Join {
                addr: peer_addr.clone(),
                buffer: w,
                is_slave,
            })
            .await;

        let _ = read_sender
            .send(ClusterReadCommand::Join {
                addr: peer_addr.clone(),
                buffer: r,
            })
            .await;
    }

    pub fn remove_peer(&mut self, peer_addr: PeerAddr) {
        self.peers.retain(|addr| addr != &peer_addr);
    }

    pub async fn handle(mut self, mut recv: Receiver<ClusterCommand>) {
        let (read_sender, lr) = tokio::sync::mpsc::channel::<ClusterReadCommand>(1000);
        let write_sender = ClusterWriteActor::run();
        tokio::spawn(run_cluster_read_actor(lr));

        while let Some(command) = recv.recv().await {
            match command {
                ClusterCommand::AddPeer {
                    peer_addr,
                    stream,
                    is_slave,
                } => {
                    self.add_peer(
                        peer_addr,
                        stream,
                        is_slave,
                        write_sender.clone(),
                        read_sender.clone(),
                    )
                    .await;
                }
                ClusterCommand::RemovePeer(peer_addr) => {
                    self.remove_peer(peer_addr);
                }

                ClusterCommand::GetPeers(callback) => {
                    // send
                    let _ = callback.send(self.peers.iter().cloned().collect());
                }
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
            ClusterReadCommand::Join { addr, buffer } => {
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
    async fn heartbeat(&mut self) {
        for connected in self.members.values_mut() {
            let _ = connected.ping().await;
        }
    }

    fn run() -> Sender<ClusterWriteCommand> {
        let (sender, mut recv) = tokio::sync::mpsc::channel::<ClusterWriteCommand>(1000);

        // send heartbeats to all peers
        let sender_clone = sender.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let _ = sender_clone.send(ClusterWriteCommand::Ping).await;
            }
        });

        tokio::spawn(async move {
            let mut actor = ClusterWriteActor::new();
            while let Some(command) = recv.recv().await {
                match command {
                    ClusterWriteCommand::Replicate { query } => {
                        // do something
                    }
                    ClusterWriteCommand::Ping => {
                        actor.heartbeat().await;
                    }
                    ClusterWriteCommand::Join {
                        addr,
                        buffer,
                        is_slave,
                    } => {
                        let connected_type = if is_slave {
                            ClusterWriteConnected::Replica { stream: buffer }
                        } else {
                            ClusterWriteConnected::ClusterMember { stream: buffer }
                        };
                        actor.members.entry(addr).or_insert(connected_type);
                    }
                }
            }
        });
        sender
    }
}

#[derive(Debug)]
pub enum ClusterWriteConnected {
    Replica { stream: OwnedWriteHalf },
    ClusterMember { stream: OwnedWriteHalf },
}
impl ClusterWriteConnected {
    async fn ping(&mut self) {
        let msg = QueryIO::SimpleString("PING".to_string()).serialize();

        match self {
            ClusterWriteConnected::Replica { stream } => {
                let _ = stream.write(msg.as_bytes()).await;
            }
            ClusterWriteConnected::ClusterMember { stream } => {
                let _ = stream.write(msg.as_bytes()).await;
            }
        }
    }
}

pub enum ClusterWriteCommand {
    Replicate {
        query: QueryIO,
    },
    Ping,
    Join {
        addr: PeerAddr,
        buffer: OwnedWriteHalf,
        is_slave: bool,
    },
}

pub enum ClusterReadCommand {
    Ping,
    Join {
        addr: PeerAddr,
        buffer: OwnedReadHalf,
    },
}
