use super::command::ClusterCommand;
use super::peer::Peer;
use super::replication::Replication;
use super::types::PeerAddr;
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Default)]
pub struct ClusterActor {
    members: BTreeMap<PeerAddr, Peer>,
    replication: Replication,
}

impl ClusterActor {
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
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
                }

                ClusterCommand::Replicate { query } => todo!(),
                ClusterCommand::Ping => {
                    self.heartbeat().await;
                }
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                }
                ClusterCommand::SetReplicationId(master_repl_id) => {
                    self.set_replication_id(master_repl_id);
                }
            }
        }
    }

    async fn heartbeat(&mut self) {
        for peer in self.members.values_mut() {
            let msg = QueryIO::SimpleString(b"PING".into()).serialize();
            let _ = peer.w_conn.stream.write(&msg).await;
        }
    }

    async fn remove_peer(&mut self, peer_addr: PeerAddr) {
        if let Some(peer) = self.members.remove(&peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.listner_kill_trigger.kill().await;
        }
        self.members.remove(&peer_addr);
    }

    fn set_replication_id(&mut self, master_repl_id: String) {
        self.replication.master_replid = master_repl_id
    }
}
