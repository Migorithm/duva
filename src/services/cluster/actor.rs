use super::command::ClusterWriteCommand;
use crate::make_smart_pointer;
use crate::services::cluster::command::{ClusterCommand, PeerKind};
use crate::services::interface::{TRead, TWrite};
use crate::services::query_io::QueryIO;
use std::collections::BTreeMap;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
pub struct ClusterActor {
    // PeerAddr is cluster ip:cluster_port of peer
    members: BTreeMap<PeerAddr, Peer>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct PeerAddr(pub String);
make_smart_pointer!(PeerAddr, String);

impl ClusterActor {
    async fn heartbeat(&mut self) {
        for peer in self.members.values_mut() {
            let msg = QueryIO::SimpleString("PING".to_string()).serialize();
            match &mut peer.write_connected {
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

    async fn remove_peer(&mut self, peer_addr: PeerAddr) {
        if let Some(peer) = self.members.remove(&peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.listener_handler.kill().await;
        }
        self.members.remove(&peer_addr);
    }

    pub async fn handle(
        mut self,
        self_handler: Sender<ClusterCommand>,
        mut cluster_message_listener: Receiver<ClusterCommand>,
    ) {
        while let Some(command) = cluster_message_listener.recv().await {
            match command {
                ClusterCommand::AddPeer { peer_addr, stream, peer_kind } => {
                    // composite
                    self.members.entry(peer_addr).or_insert(Peer::new(
                        stream,
                        peer_kind,
                        self_handler.clone(),
                    ));
                }
                ClusterCommand::RemovePeer(peer_addr) => {
                    self.remove_peer(peer_addr).await;
                }

                ClusterCommand::GetPeers(callback) => {
                    // send
                    let _ = callback.send(self.members.keys().cloned().collect());
                }
                ClusterCommand::Write(write_cmd) => match write_cmd {
                    ClusterWriteCommand::Replicate { query: _ } => todo!(),
                    ClusterWriteCommand::Ping => {
                        self.heartbeat().await;
                    }
                },
            }
        }
    }
}

#[derive(Debug)]
struct Peer {
    write_connected: ClusterWriteConnected,
    listener_handler: PeerListenerHandler,
}

impl Peer {
    pub fn new(
        stream: TcpStream,
        peer_kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Self {
        let (r, w) = stream.into_split();
        let (write_connected, read_connected) = match peer_kind {
            PeerKind::Peer => (
                ClusterWriteConnected::Peer { stream: w },
                ClusterReadConnected::Peer { stream: r },
            ),
            PeerKind::Replica => (
                ClusterWriteConnected::Replica { stream: w },
                ClusterReadConnected::Replica { stream: r },
            ),
            PeerKind::Master => (
                ClusterWriteConnected::Master { stream: w },
                ClusterReadConnected::Master { stream: r },
            ),
        };

        // Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let listener = PeerListener { read_connected, cluster_handler };
        let listener_task_handler = tokio::spawn(listener.listen(kill_switch));

        Self {
            write_connected,
            listener_handler: PeerListenerHandler(kill_trigger, listener_task_handler),
        }
    }
}

#[derive(Debug)]
pub enum ClusterWriteConnected {
    Replica { stream: OwnedWriteHalf },
    Peer { stream: OwnedWriteHalf },
    Master { stream: OwnedWriteHalf },
}

struct PeerListener {
    read_connected: ClusterReadConnected,
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
    // TODO only outline is done
    async fn listen(mut self, rx: ListenerKillSwitch) -> ClusterReadConnected {
        let connected = select! {
            _ = async{
                    match self.read_connected {
                        ClusterReadConnected::Replica { ref mut stream } =>Self::listen_replica_stream( stream ).await,
                        ClusterReadConnected::Peer { ref mut stream } =>Self::listen_peer_stream( stream ).await,
                        ClusterReadConnected::Master { ref mut stream } => Self::listen_master_stream( stream ).await,
                    };
                } => {
                    self.read_connected
                },
            _ = rx => {
                // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
                self.read_connected
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
