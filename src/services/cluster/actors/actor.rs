use super::command::{ClusterCommand, ClusterWriteCommand};
use super::listening_actor::{ClusterReadConnected, ListeningActorKillTrigger, PeerListeningActor};
use super::replication::Replication;
use super::types::{PeerAddr, PeerKind};

use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use std::collections::BTreeMap;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;

use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Default)]
pub struct ClusterActor {
    members: BTreeMap<PeerAddr, Peer>,
    replication: Replication,
}

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
            let _read_connected = peer.listening_actor_kill_trigger.kill().await;
        }
        self.members.remove(&peer_addr);
    }

    pub async fn handle(
        mut self,
        self_handler: Sender<ClusterCommand>,
        mut cluster_message_listener: Receiver<ClusterCommand>,
        notifier: tokio::sync::watch::Sender<bool>,
    ) {
        while let Some(command) = cluster_message_listener.recv().await {
            // TODO notifier will be used when election process is implemented
            let _ = notifier.clone();

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
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                }
                ClusterCommand::SetReplicationId(master_repl_id) => {
                    self.replication.master_replid = master_repl_id
                }
            }
        }
    }
}

#[derive(Debug)]
struct Peer {
    write_connected: ClusterWriteConnected,
    listening_actor_kill_trigger: ListeningActorKillTrigger,
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
        let listening_actor = PeerListeningActor { read_connected, cluster_handler };
        let listening_task = tokio::spawn(listening_actor.listen(kill_switch));

        Self {
            write_connected,
            listening_actor_kill_trigger: ListeningActorKillTrigger::new(
                kill_trigger,
                listening_task,
            ),
        }
    }
}

#[derive(Debug)]
pub enum ClusterWriteConnected {
    Replica { stream: OwnedWriteHalf },
    Peer { stream: OwnedWriteHalf },
    Master { stream: OwnedWriteHalf },
}
