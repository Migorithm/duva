use crate::domains::{
    append_only_files::{WriteOperation, WriteRequest},
    query_parsers::QueryIO,
};

use super::{
    commands::AddPeer,
    replication::{time_in_secs, BannedPeer, HeartBeatMessage, ReplicationInfo},
    *,
};

#[derive(Debug)]
pub struct ClusterActor {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationInfo,
    pub(crate) node_timeout: u128,
    notifier: tokio::sync::watch::Sender<bool>,
}

impl ClusterActor {
    pub fn new(
        node_timeout: u128,
        init_repl_info: ReplicationInfo,
        notifier: tokio::sync::watch::Sender<bool>,
    ) -> Self {
        Self { members: BTreeMap::new(), replication: init_repl_info, node_timeout, notifier }
    }

    pub fn hop_count(&self, fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }

    pub async fn send_liveness_heartbeat(&mut self, hop_count: u8) {
        // TODO randomly choose the peer to send the message

        for peer in self.members.values_mut() {
            let msg = QueryIO::HeartBeat(self.replication.default_heartbeat(hop_count)).serialize();

            let _ = peer.write(msg).await;
        }
    }

    pub async fn add_peer(&mut self, add_peer_cmd: AddPeer) {
        let AddPeer { peer_id: peer_addr, peer } = add_peer_cmd;

        self.replication.remove_from_ban_list(&peer_addr);

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer_addr.clone(), peer) {
            // stop the runnin process and take the connection in case topology changes are made
            existing_peer.listener_kill_trigger.kill().await;
        }
    }
    pub async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.remove(peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.listener_kill_trigger.kill().await;
            return Some(());
        }
        None
    }

    pub fn set_replication_info(&mut self, leader_repl_id: String, offset: u64) {
        self.replication.leader_repl_id = leader_repl_id;
        self.replication.leader_repl_offset = offset;
    }

    /// Remove the peers that are idle for more than ttl_mills
    pub async fn remove_idle_peers(&mut self) {
        // loop over members, if ttl is expired, remove the member
        let now = Instant::now();

        let to_be_removed = self
            .members
            .iter()
            .filter(|&(_, peer)| now.duration_since(peer.last_seen).as_millis() > self.node_timeout)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();

        for peer_id in to_be_removed {
            self.remove_peer(&peer_id).await;
        }
    }

    pub async fn gossip(&mut self, hop_count: u8) {
        // If hop_count is 0, don't send the message to other peers
        if hop_count == 0 {
            return;
        };
        let hop_count = hop_count - 1;
        self.send_liveness_heartbeat(hop_count).await;
    }

    pub async fn forget_peer(&mut self, peer_addr: PeerIdentifier) -> anyhow::Result<Option<()>> {
        self.replication.ban_peer(&peer_addr)?;

        Ok(self.remove_peer(&peer_addr).await)
    }

    pub fn merge_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
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

    pub async fn update_on_report(&mut self, state: HeartBeatMessage) {
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

    pub async fn req_consensus(&mut self, req: WriteRequest) {
        // TODO when are we going to increase offset?

        let heartbeat = self.replication.append_entry(0, req);

        let mut tasks = self
            .followers_mut()
            .into_iter()
            .map(|peer| peer.write_io(heartbeat.clone()))
            .collect::<FuturesUnordered<_>>();

        // ! SAFETY DO NOT inline tasks.next().await in the while loop
        while let Some(_) = tasks.next().await {}
    }

    pub(crate) fn leader_mut(&mut self) -> Option<&mut Peer> {
        self.members.values_mut().find(|peer| matches!(peer.w_conn.kind, PeerKind::Leader))
    }

    pub(crate) fn followers(&self) -> impl Iterator<Item = &Peer> {
        self.members
            .values()
            .into_iter()
            .filter(|peer| matches!(peer.w_conn.kind, PeerKind::Follower))
    }

    pub(crate) fn followers_mut(&mut self) -> Vec<&mut Peer> {
        self.members
            .values_mut()
            .into_iter()
            .filter(|peer| matches!(peer.w_conn.kind, PeerKind::Follower))
            .collect::<Vec<_>>()
    }

    pub fn apply_acks(&self, consensus_con: &mut ConsensusTracker, offsets: Vec<u64>) {
        offsets.into_iter().for_each(|offset| {
            if let Some(mut consensus) = consensus_con.take(&offset) {
                println!("[INFO] Received acks for offset: {}", offset);
                consensus.apply_vote();

                if let Some(consensus) = consensus.maybe_not_finished(offset) {
                    consensus_con.insert(offset, consensus);
                }
            }
        });
    }

    pub async fn receive_log_entries_from_leader(&mut self, write_operations: Vec<WriteOperation>) {
        let offsets = write_operations.iter().map(|op| op.offset).collect::<Vec<_>>();
        if let Some(leader) = self.leader_mut() {
            let _ = leader.write_io(QueryIO::Acks(offsets)).await;
        }
    }
}

#[test]
fn test_hop_count_when_one() {
    // GIVEN
    let fanout = 2;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let replication = ReplicationInfo::new(None, "localhost", 8080);
    let cluster_actor = ClusterActor::new(100, replication, tx);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 1);
    // THEN
    assert_eq!(hop_count, 0);
}

#[test]
fn test_hop_count_when_two() {
    // GIVEN
    let fanout = 2;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let replication = ReplicationInfo::new(None, "localhost", 8080);
    let cluster_actor = ClusterActor::new(100, replication, tx);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 2);
    // THEN
    assert_eq!(hop_count, 0);
}

#[test]
fn test_hop_count_when_three() {
    // GIVEN
    let fanout = 2;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let replication = ReplicationInfo::new(None, "localhost", 8080);
    let cluster_actor = ClusterActor::new(100, replication, tx);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 3);
    // THEN
    assert_eq!(hop_count, 1);
}

#[test]
fn test_hop_count_when_thirty() {
    // GIVEN
    let fanout = 2;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let replication = ReplicationInfo::new(None, "localhost", 8080);
    let cluster_actor = ClusterActor::new(100, replication, tx);

    // WHEN
    let hop_count = cluster_actor.hop_count(fanout, 30);
    // THEN
    assert_eq!(hop_count, 4);
}
