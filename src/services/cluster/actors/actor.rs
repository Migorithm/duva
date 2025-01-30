use super::command::AddPeer;
use super::command::ClusterCommand;
use super::peer::Peer;
use super::replication::Replication;
use super::types::PeerIdentifier;
use super::PeerState;
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub struct ClusterActor {
    members: BTreeMap<PeerIdentifier, Peer>,
    replication: Replication,
    ttl_mills: u128,
}

impl ClusterActor {
    pub fn new(ttl_mills: u128) -> Self {
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

                    // ! remove idle peers based on ttl.
                    // ! The following may need to be moved else where to avoid blocking the main loop
                    self.remove_idle_peers().await;
                }
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                }
                ClusterCommand::SetReplicationInfo { master_repl_id, offset } => {
                    self.set_replication_info(master_repl_id, offset);
                }
                ClusterCommand::ReportAlive { peer_identifier, state } => {
                    self.update_peer_state(peer_identifier, state);
                }
            }
        }
    }

    fn hop_count(node_count: usize) -> u8 {
        // TODO FANOUT should be configurable
        const FANOUT: f32 = 2.0;
        (node_count as f32).log(FANOUT).ceil() as u8
    }
    async fn send_heartbeat(&mut self) {
        if self.members.is_empty() {
            return;
        }

        let hop_count = Self::hop_count(self.members.len());

        for peer in self.members.values_mut() {
            let msg = QueryIO::PeerState(self.replication.current_state(hop_count)).serialize();
            let _ = peer.w_conn.stream.write(&msg).await;
        }
    }

    fn add_peer(&mut self, add_peer_cmd: AddPeer, self_handler: Sender<ClusterCommand>) {
        let AddPeer { peer_addr, stream, peer_kind } = add_peer_cmd;
        self.members.entry(peer_addr.clone()).or_insert(Peer::new(
            stream,
            peer_kind,
            self_handler.clone(),
            peer_addr,
        ));
    }
    async fn remove_peer(&mut self, peer_addr: PeerIdentifier) {
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

    fn update_peer_state(&mut self, peer_identifier: PeerIdentifier, state: PeerState) {
        let Some(peer) = self.members.get_mut(&peer_identifier) else {
            eprintln!("Peer not found");
            return;
        };
        peer.last_seen = std::time::Instant::now();
    }

    /// Remove the peers that are idle for more than ttl_mills
    async fn remove_idle_peers(&mut self) {
        // loop over members, if ttl is expired, remove the member
        let now = std::time::Instant::now();

        let to_be_removed = self
            .members
            .iter()
            .filter_map(|(id, peer)| {
                (now.duration_since(peer.last_seen).as_millis() > self.ttl_mills)
                    .then(|| id.clone())
            })
            .collect::<Vec<_>>();

        for peer_id in to_be_removed {
            self.remove_peer(peer_id).await;
        }
    }
}

#[test]
fn test_hop_count_when_one() {
    // GIVEN
    let node_count = 1;
    // WHEN
    let hop_count = ClusterActor::hop_count(node_count);
    // THEN
    assert_eq!(hop_count, 0);
}

#[test]
fn test_hop_count_when_two() {
    // GIVEN
    let node_count = 2;
    // WHEN
    let hop_count = ClusterActor::hop_count(node_count);
    // THEN
    assert_eq!(hop_count, 1);
}

#[test]
fn test_hop_count_when_thirty() {
    // GIVEN
    let node_count = 30;
    // WHEN
    let hop_count = ClusterActor::hop_count(node_count);
    // THEN
    assert_eq!(hop_count, 5);
}
