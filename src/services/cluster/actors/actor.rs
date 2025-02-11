use crate::services::aof::{WriteOperation, WriteRequest};
use crate::services::cluster::command::cluster_command::{AddPeer, ClusterCommand};
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::peers::peer::Peer;
use crate::services::cluster::replications::replication::{
    time_in_secs, BannedPeer, PeerState, ReplicationInfo,
};
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::snapshot::manager::SnapshotManager;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

#[derive(Debug)]
pub struct ClusterActor {
    members: BTreeMap<PeerIdentifier, Peer>,
    replication: ReplicationInfo,
    ttl_mills: u128,
}

impl ClusterActor {
    pub fn new(ttl_mills: u128) -> Self {
        Self { members: BTreeMap::new(), replication: ReplicationInfo::default(), ttl_mills }
    }
    pub async fn handle(
        mut self,
        self_handler: Sender<ClusterCommand>,
        mut cluster_message_listener: Receiver<ClusterCommand>,
        notifier: tokio::sync::watch::Sender<bool>,
        snapshot_manager: SnapshotManager,
    ) {
        while let Some(command) = cluster_message_listener.recv().await {
            // TODO notifier will be used when election process is implemented
            let _ = notifier.clone();

            match command {
                ClusterCommand::AddPeer(add_peer_cmd) => {
                    self.add_peer(add_peer_cmd, self_handler.clone(), snapshot_manager.clone()).await;
                }

                ClusterCommand::GetPeers(callback) => {
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
                }

                ClusterCommand::Replicate { query: _ } => todo!(),
                ClusterCommand::SendHeartBeat => {
                    // TODO FANOUT should be configurable
                    const FANOUT: usize = 2;
                    let hop_count = self.hop_count(FANOUT, self.members.len());

                    self.send_heartbeat(hop_count).await;

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
                ClusterCommand::ReportAlive { state } => {
                    if self.replication.in_ban_list(&state.heartbeat_from) {
                        continue;
                    }

                    self.gossip(state.hop_count).await;
                    self.update_on_report(state).await;
                }
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                }
                ClusterCommand::Concensus { log, sender } => {
                    // TODO logging

                    self.consensus(log).await;
                    // TODO implement concensus
                    // TODO if any operations failed, it's okay to drop sender
                    let _ = sender.send(self.replication.master_repl_offset);
                }
                ClusterCommand::CommitLog(commit_log) => {
                    // TODO send commit to all replicas
                }
            }
        }
    }

    fn hop_count(&self, fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }
    async fn send_heartbeat(&mut self, hop_count: u8) {
        // TODO randomly choose the peer to send the message

        for peer in self.members.values_mut() {
            let msg = QueryIO::PeerState(self.replication.current_state(hop_count)).serialize();

            let _ = peer.w_conn.stream.write(msg).await;
        }
    }

    async fn add_peer(&mut self, add_peer_cmd: AddPeer, self_handler: Sender<ClusterCommand>, snapshot_manager: SnapshotManager) {
        let AddPeer { peer_addr, stream, peer_kind } = add_peer_cmd;

        self.replication.remove_from_ban_list(&peer_addr);

        let peer = Peer::new(stream, peer_kind, self_handler, snapshot_manager, peer_addr.clone());

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer_addr.clone(), peer) {
            // stop the runnin process and take the connection in case topology changes are made
            existing_peer.listener_kill_trigger.kill().await;
        }
    }
    async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.remove(peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.listener_kill_trigger.kill().await;
            return Some(());
        }
        None
    }

    fn set_replication_info(&mut self, master_repl_id: String, offset: u64) {
        self.replication.master_replid = master_repl_id;
        self.replication.master_repl_offset = offset;
    }

    /// Remove the peers that are idle for more than ttl_mills
    async fn remove_idle_peers(&mut self) {
        // loop over members, if ttl is expired, remove the member
        let now = Instant::now();

        let to_be_removed = self
            .members
            .iter()
            .filter(|&(_, peer)| (now.duration_since(peer.last_seen).as_millis() > self.ttl_mills))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();

        for peer_id in to_be_removed {
            self.remove_peer(&peer_id).await;
        }
    }

    async fn gossip(&mut self, hop_count: u8) {
        // If hop_count is 0, don't send the message to other peers
        if hop_count == 0 {
            return;
        };
        let hop_count = hop_count - 1;
        self.send_heartbeat(hop_count).await;
    }

    async fn forget_peer(&mut self, peer_addr: PeerIdentifier) -> anyhow::Result<Option<()>> {
        self.replication.ban_peer(&peer_addr)?;

        Ok(self.remove_peer(&peer_addr).await)
    }

    fn merge_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        if ban_list.is_empty() {
            return;
        }
        // merge, deduplicate and retain the latest
        self.replication.ban_list.extend(ban_list);
        self.replication
            .ban_list
            .sort_by_key(|node| (node.p_id.clone(), std::cmp::Reverse(node.ban_time)));
        self.replication.ban_list.dedup_by_key(|node| node.p_id.clone());
    }

    async fn update_on_report(&mut self, state: PeerState) {
        let Some(peer) = self.members.get_mut(&state.heartbeat_from) else {
            return;
        };
        // update peer
        peer.last_seen = Instant::now();
        self.merge_ban_list(state.ban_list);

        let current_time_in_sec = time_in_secs().unwrap();
        self.replication.ban_list.retain(|node| current_time_in_sec - node.ban_time < 60);
        if self.replication.ban_list.is_empty() {
            return;
        }
        for node in
            self.replication.ban_list.iter().map(|node| node.p_id.clone()).collect::<Vec<_>>()
        {
            self.remove_peer(&node).await;
        }
    }

    async fn consensus(&mut self, log: WriteRequest) {
        // TODO send current offset
        let write_op = WriteOperation { op: log, offset: self.replication.master_repl_offset };

        for peer in self.replicas() {
            let _ = peer.w_conn.stream.write(write_op.clone().serialize()).await;
        }
        // TODO implement concensus
        // TODO if any operations failed, it's okay to drop sender
    }

    fn replicas(&mut self) -> Vec<&mut Peer> {
        self.members
            .values_mut()
            .into_iter()
            .filter(|peer| matches!(peer.w_conn.kind, PeerKind::Replica))
            .collect::<Vec<_>>()
    }
}

#[test]
fn test_hop_count_when_one() {
    // GIVEN
    let fanout = 2;
    let cluster_actor = ClusterActor::new(100);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 1);
    // THEN
    assert_eq!(hop_count, 0);
}

#[test]
fn test_hop_count_when_two() {
    // GIVEN
    let fanout = 2;
    let cluster_actor = ClusterActor::new(100);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 2);
    // THEN
    assert_eq!(hop_count, 0);
}

#[test]
fn test_hop_count_when_three() {
    // GIVEN
    let fanout = 2;
    let cluster_actor = ClusterActor::new(100);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 3);
    // THEN
    assert_eq!(hop_count, 1);
}

#[test]
fn test_hop_count_when_thirty() {
    // GIVEN
    let fanout = 2;
    let cluster_actor = ClusterActor::new(100);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 30);
    // THEN
    assert_eq!(hop_count, 4);
}
