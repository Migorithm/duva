use super::command::ClusterWriteCommand;
use crate::make_smart_pointer;
use crate::services::cluster::command::{ClusterCommand, PeerKind};
use crate::services::interface::{TRead, TWrite};
use crate::services::query_io::QueryIO;
use std::collections::{BTreeMap, HashSet};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
pub struct ClusterActor {
    // TODO change PeerAddr to PeerIdentifier
    pub peers: HashSet<PeerAddr>,
    write_members: BTreeMap<PeerAddr, ClusterWriteConnected>,
    read_members: BTreeMap<PeerAddr, PeerListenerHandler>,
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

    async fn remove_peer(&mut self, peer_addr: PeerAddr) {
        if let Some(connected) = self.read_members.remove(&peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = connected.kill().await;
        }
        self.write_members.remove(&peer_addr);
    }

    pub async fn handle(
        mut self,
        self_handler: Sender<ClusterCommand>,
        mut cluster_message_listener: Receiver<ClusterCommand>,
    ) {
        while let Some(command) = cluster_message_listener.recv().await {
            match command {
                ClusterCommand::AddPeer { peer_addr, stream, peer_kind } => {
                    let (r, w) = stream.into_split();
                    // spawn listener
                    let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
                    let listener = PeerListener::new(r, &peer_kind, self_handler.clone());
                    let listner_task_handler = tokio::spawn(listener.listen(kill_switch));

                    // composite
                    self.read_members.insert(
                        peer_addr.clone(),
                        PeerListenerHandler(kill_trigger, listner_task_handler),
                    );

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
                ClusterCommand::Write(write_cmd) => match write_cmd {
                    ClusterWriteCommand::Replicate { query } => todo!(),
                    ClusterWriteCommand::Ping => {
                        self.heartbeat().await;
                    }
                },
            }
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
struct PeerListener {
    connected: ClusterReadConnected,
    cluster_handler: Sender<ClusterCommand>, // cluster_handler is used to send messages to the cluster actor
}

#[derive(Debug)]
struct PeerListenerHandler(ListenerKillTrigger, JoinHandle<ClusterReadConnected>);
impl PeerListenerHandler {
    async fn kill(self) -> ClusterReadConnected {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}

type ListenerKillTrigger = tokio::sync::oneshot::Sender<()>;
type ListenerKillSwitch = tokio::sync::oneshot::Receiver<()>;

impl PeerListener {
    pub fn new(
        stream: OwnedReadHalf,
        peer_kind: &PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Self {
        Self {
            connected: match peer_kind {
                PeerKind::Peer => ClusterReadConnected::Peer { stream },
                PeerKind::Replica => ClusterReadConnected::Replica { stream },
                PeerKind::Master => ClusterReadConnected::Master { stream },
            },
            cluster_handler,
        }
    }

    // TODO only outline is done
    async fn listen(mut self, rx: ListenerKillSwitch) -> ClusterReadConnected {
        let connected = select! {
            _ = async{
                    match self.connected {
                        ClusterReadConnected::Replica { ref mut stream } =>Self::listen_replica_stream( stream).await,
                        ClusterReadConnected::Peer { ref mut stream } =>Self::listen_peer_stream( stream).await,
                        ClusterReadConnected::Master { ref mut stream } => Self::listen_master_stream( stream).await,
                    };
                } => {
                    self.connected
                },
            _ = rx => {
                // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
                self.connected
            }
        };
        connected
    }

    async fn listen_replica_stream(read_buf: &mut OwnedReadHalf) {
        while let Ok(values) = read_buf.read_values().await {
            let _ = values;
        }
    }
    async fn listen_peer_stream(read_buf: &mut OwnedReadHalf) {
        while let Ok(values) = read_buf.read_values().await {
            let _ = values;
        }
    }
    async fn listen_master_stream(read_buf: &mut OwnedReadHalf) {
        while let Ok(values) = read_buf.read_values().await {
            let _ = values;
        }
    }
}

#[derive(Debug)]
enum ClusterReadConnected {
    Replica { stream: OwnedReadHalf },
    Peer { stream: OwnedReadHalf },
    Master { stream: OwnedReadHalf },
}
