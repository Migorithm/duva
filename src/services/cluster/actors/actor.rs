use crate::services::cluster::command::cluster_command::{AddPeer, ClusterCommand};
use crate::services::cluster::peer::identifier::PeerIdentifier;
use crate::services::cluster::peer::peer::Peer;
use crate::services::cluster::replication::replication::{PeerState, Replication};
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
                    self.update_peer_state(&state);
                    self.gossip(state).await;
                }
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    let peer = self.remove_peer(peer_addr).await;
                    if let Some(_) = peer {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                }
            }
        }
    }

    fn hop_count(&self, fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout as usize {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }
    async fn send_heartbeat(&mut self, hop_count: u8) {
        // TODO randomly choose the peer to send the message

        for peer in self.members.values_mut() {
            let msg = QueryIO::PeerState(self.replication.current_state(hop_count)).serialize();

            let _ = peer.w_conn.stream.write(&msg).await;
        }
    }

    fn add_peer(&mut self, add_peer_cmd: AddPeer, self_handler: Sender<ClusterCommand>) {
        let AddPeer { peer_addr, stream, peer_kind } = add_peer_cmd;

        println!("Peer added: {}", peer_addr);
        self.members.entry(peer_addr.clone()).or_insert(Peer::new(
            stream,
            peer_kind,
            self_handler.clone(),
            peer_addr,
        ));
    }
    async fn remove_peer(&mut self, peer_addr: PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.remove(&peer_addr) {
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

    async fn gossip(&mut self, state: PeerState) {
        if state.hop_count == 0 {
            return;
        };
        let hop_count = state.hop_count - 1;
        self.send_heartbeat(hop_count).await;
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
