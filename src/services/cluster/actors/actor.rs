use crate::services::cluster::command::cluster_command::{AddPeer, ClusterCommand};
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::peer::Peer;
use crate::services::cluster::replications::replication::{PeerState, Replication};
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

#[derive(Debug)]
pub struct ClusterActor {
    members: BTreeMap<PeerIdentifier, Peer>,
    replication: Replication,
    ttl_mills: u128,
    ban_list: Vec<PeerIdentifier>,
}

impl ClusterActor {
    pub fn new(ttl_mills: u128) -> Self {
        Self {
            members: BTreeMap::new(),
            replication: Replication::default(),
            ttl_mills,
            ban_list: Default::default(),
        }
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
                    // ! If it is already in ban-list, don't add
                    if self.in_ban_list(&add_peer_cmd.peer_addr) {
                        return;
                    }
                    self.add_peer(add_peer_cmd, self_handler.clone());
                }
                ClusterCommand::RemovePeer(peer_addr) => {
                    self.remove_peer(&peer_addr).await;
                }

                ClusterCommand::GetPeers(callback) => {
                    // send
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
                    if self.in_ban_list(&state.id) {
                        return;
                    }
                    self.gossip(state).await;
                }
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    let removed = self.forget_peer(peer_addr, self_handler.clone()).await;

                    if removed.is_some() {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                }
                ClusterCommand::LiftBan(peer_identifier) => {
                    self.ban_list.retain(|x| x != &peer_identifier)
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

    fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        self.ban_list.iter().find(|node| *node == peer_identifier).is_some()
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

    fn update_peer_state(&mut self, state: &PeerState) {
        let Some(peer) = self.members.get_mut(&state.id) else {
            eprintln!("Peer not found {}", state.id);
            println!("Peers: {:?}", self.members.keys());
            return;
        };
        peer.last_seen = Instant::now();
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

    async fn gossip(&mut self, state: PeerState) {
        self.update_peer_state(&state);

        if state.hop_count == 0 {
            return;
        };
        let hop_count = state.hop_count - 1;
        self.send_heartbeat(hop_count).await;
    }

    async fn forget_peer(
        &mut self,
        peer_addr: PeerIdentifier,
        self_handler: Sender<ClusterCommand>,
    ) -> Option<()> {
        self.ban_list.push(peer_addr.clone());

        // TODO propagate forget information in gossip.

        tokio::spawn({
            let pr = peer_addr.clone();
            async move { self_handler.send(ClusterCommand::LiftBan(pr)).await }
        });

        self.remove_peer(&peer_addr).await
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
