use super::command::AddPeer;
use super::command::ClusterCommand;
use super::peer::Peer;
use super::replication::Replication;
use super::types::PeerAddr;
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct ClusterActor {
    members: BTreeMap<PeerAddr, Peer>,
    replication: Replication,
    ttl_mills: u64,
}

impl ClusterActor {
    pub fn new(ttl_mills: u64) -> Self {
        Self { members: BTreeMap::new(), replication: Replication::default(), ttl_mills }
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
                ClusterCommand::AddPeer(add_peer_cmd) => {
                    // composite
                    self.add_peer(add_peer_cmd, self_handler.clone());
                }
                ClusterCommand::RemovePeer(peer_addr) => {
                    self.remove_peer(peer_addr).await;
                }

                ClusterCommand::GetPeers(callback) => {
                    // send
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
                }

                ClusterCommand::Replicate { query } => todo!(),
                ClusterCommand::SendHeartBeat => {
                    self.send_heartbeat().await;
                }
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                }
                ClusterCommand::SetReplicationInfo { master_repl_id, offset } => {
                    self.set_replication_info(master_repl_id, offset);
                }
            }
        }
    }

    async fn send_heartbeat(&mut self) {
        for peer in self.members.values_mut() {
            let msg = QueryIO::PeerState(self.replication.current_state()).serialize();
            let _ = peer.w_conn.stream.write(&msg).await;
        }
    }

    fn add_peer(&mut self, add_peer_cmd: AddPeer, self_handler: Sender<ClusterCommand>) {
        let AddPeer { peer_addr, stream, peer_kind } = add_peer_cmd;
        self.members.entry(peer_addr).or_insert(Peer::new(stream, peer_kind, self_handler.clone()));
    }
    async fn remove_peer(&mut self, peer_addr: PeerAddr) {
        if let Some(peer) = self.members.remove(&peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.listner_kill_trigger.kill().await;
        }
        self.members.remove(&peer_addr);
    }

    fn set_replication_info(&mut self, master_repl_id: String, offset: u64) {
        self.replication.master_replid = master_repl_id;
        self.replication.master_repl_offset = offset;
    }
}
