use crate::domains::{
    append_only_files::{WriteOperation, interfaces::TAof, log::LogIndex, logger::Logger},
    caches::cache_manager::CacheManager,
    query_parsers::QueryIO,
};

use super::{
    commands::AddPeer,
    replication::{BannedPeer, HeartBeatMessage, ReplicationInfo, time_in_secs},
    *,
};

#[derive(Debug)]
pub struct ClusterActor {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationInfo,
    pub(crate) node_timeout: u128,
    pub(crate) cache_manager: CacheManager,
    pub(crate) consensus_con: ConsensusTracker,
    notifier: tokio::sync::watch::Sender<bool>,
}

impl ClusterActor {
    pub fn new(
        node_timeout: u128,
        init_repl_info: ReplicationInfo,
        cache_manager: CacheManager,
        notifier: tokio::sync::watch::Sender<bool>,
    ) -> Self {
        Self {
            members: BTreeMap::new(),
            replication: init_repl_info,
            node_timeout,
            notifier,
            consensus_con: ConsensusTracker::default(),
            cache_manager,
        }
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
        self.replication.commit_idx = offset;
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

    pub fn update_last_seen(&mut self, peer_id: &PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.get_mut(peer_id) {
            peer.last_seen = Instant::now();
            return Some(());
        }
        None
    }

    pub async fn update_on_report(&mut self, state: HeartBeatMessage) {
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

    pub(crate) async fn req_consensus(
        &mut self,
        logger: &mut Logger<impl TAof>,
        sender: tokio::sync::oneshot::Sender<Option<LogIndex>>,
        append_entries: Vec<WriteOperation>,
    ) {
        // Skip consensus for no replicas
        let repl_count = self.followers().count();
        if repl_count == 0 {
            let _ = sender.send(None);
            return;
        }
        self.consensus_con.add(logger.log_index, sender, repl_count);

        // create entries per follower.
        let default_heartbeat = self.replication.default_heartbeat(0);

        let mut tasks = self
            .followers_mut()
            .into_iter()
            .map(|(peer, commit_idx)| {
                let logs = append_entries
                    .iter()
                    .filter(|op| *op.log_index > commit_idx)
                    .cloned()
                    .collect::<Vec<_>>();
                peer.write_io(default_heartbeat.clone().set_append_entries(logs))
            })
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
            .filter(|peer| matches!(peer.w_conn.kind, PeerKind::Follower(_)))
    }

    pub(crate) fn followers_mut(&mut self) -> Vec<(&mut Peer, u64)> {
        self.members
            .values_mut()
            .into_iter()
            .filter_map(|peer| match peer.w_conn.kind {
                PeerKind::Follower(current_commit) => Some((peer, current_commit)),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    pub(crate) fn get_lowerest_commit_idx(&self) -> u64 {
        self.members
            .values()
            .into_iter()
            .filter_map(|peer| match peer.w_conn.kind {
                PeerKind::Follower(current_commit) => Some(current_commit),
                _ => None,
            })
            .min()
            .unwrap_or(0)
    }

    pub fn apply_acks(&mut self, offsets: Vec<LogIndex>) {
        offsets.into_iter().for_each(|offset| {
            if let Some(mut consensus) = self.consensus_con.take(&offset) {
                println!("[INFO] Received acks for log index num: {}", offset);
                consensus.apply_vote();

                if let Some(consensus) = consensus.maybe_not_finished(offset) {
                    self.consensus_con.insert(offset, consensus);
                }
            }
        });
    }

    pub(crate) async fn send_ack(&mut self, offset: LogIndex) {
        if let Some(leader) = self.leader_mut() {
            // TODO send the last offset instead of multiple offsets.
            let _ = leader.write_io(QueryIO::Acks(vec![offset])).await;
        }
    }

    pub(crate) async fn send_commit_heartbeat(&mut self, offset: LogIndex) {
        // TODO is there any case where I can use offset input?
        self.replication.commit_idx += 1;
        let message: HeartBeatMessage = self.replication.default_heartbeat(0);
        println!("[INFO] Sending commit request on {}", message.commit_idx);

        let mut tasks = self
            .followers_mut()
            .into_iter()
            .map(|(peer, _)| peer.write_io(message.clone()))
            .collect::<FuturesUnordered<_>>();

        while let Some(_) = tasks.next().await {}
    }

    pub(crate) async fn replicate(
        &mut self,
        logger: &mut Logger<impl TAof>,
        heartbeat: HeartBeatMessage,
    ) {
        // * lagging case
        if self.replication.commit_idx < heartbeat.commit_idx {
            println!("[INFO] Received commit offset {}", heartbeat.commit_idx);
            //* Retrieve the logs that fall between the current 'log' index of this node and leader 'commit' idx
            for log in logger.range(self.replication.commit_idx, heartbeat.commit_idx) {
                let _ = self.cache_manager.apply_log(log.request).await;
                self.replication.commit_idx = log.log_index.into();
            }
        }

        // * logging case
        if heartbeat.append_entries.is_empty() || self.replication.term > heartbeat.term {
            return;
        }
        let Ok(ack_index) = logger.write_log_entries(heartbeat.append_entries.clone()).await else {
            return;
        };
        self.send_ack(ack_index).await;
    }
}

#[cfg(test)]
mod test {
    use crate::{
        adapters::aof::memory_aof::InMemoryAof,
        domains::{
            append_only_files::WriteRequest,
            caches::{cache_objects::CacheEntry, command::CacheCommand},
        },
    };

    use super::*;
    #[test]
    fn test_hop_count_when_one() {
        // GIVEN
        let fanout = 2;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let replication = ReplicationInfo::new(None, "localhost", 8080);
        let cluster_actor =
            ClusterActor::new(100, replication, CacheManager { inboxes: vec![] }, tx);

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
        let cluster_actor =
            ClusterActor::new(100, replication, CacheManager { inboxes: vec![] }, tx);

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
        let cluster_actor =
            ClusterActor::new(100, replication, CacheManager { inboxes: vec![] }, tx);

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
        let cluster_actor =
            ClusterActor::new(100, replication, CacheManager { inboxes: vec![] }, tx);

        // WHEN
        let hop_count = cluster_actor.hop_count(fanout, 30);
        // THEN
        assert_eq!(hop_count, 4);
    }

    fn write_operation_create_helper(index_num: u64, key: &str, value: &str) -> WriteOperation {
        WriteOperation {
            log_index: index_num.into(),
            request: WriteRequest::Set { key: key.into(), value: value.into() },
        }
    }
    fn heartbeat_create_helper(
        term: u64,
        commit_idx: u64,
        op_logs: Vec<WriteOperation>,
    ) -> HeartBeatMessage {
        HeartBeatMessage {
            term,
            commit_idx,
            append_entries: op_logs,
            ban_list: vec![],
            heartbeat_from: PeerIdentifier::new("localhost", 8080),
            leader_replid: "localhost".to_string(),
            hop_count: 0,
        }
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_log() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());
        let (notifier, _) = tokio::sync::watch::channel(false);
        let repl_info = ReplicationInfo::new(None, "localhost", 8080);
        let mut cluster_actor =
            ClusterActor::new(100, repl_info, CacheManager { inboxes: vec![] }, notifier);

        // WHEN - term
        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, "foo", "bar"),
                write_operation_create_helper(2, "foo2", "bar"),
            ],
        );

        cluster_actor.replicate(&mut test_logger, heartbeat).await;

        // THEN
        assert_eq!(cluster_actor.replication.commit_idx, 0);
        assert_eq!(test_logger.log_index, 2.into());
        let logs = test_logger.range(0, 2);
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 1.into());
        assert_eq!(logs[1].log_index, 2.into());
        assert_eq!(logs[0].request, WriteRequest::Set { key: "foo".into(), value: "bar".into() });
        assert_eq!(logs[1].request, WriteRequest::Set { key: "foo2".into(), value: "bar".into() });
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());
        let (notifier, _) = tokio::sync::watch::channel(false);
        let repl_info = ReplicationInfo::new(None, "localhost", 8080);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let cache_actor: CacheManager = CacheManager::test_new(tx);
        let mut cluster_actor = ClusterActor::new(100, repl_info, cache_actor, notifier);
        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, "foo", "bar"),
                write_operation_create_helper(2, "foo2", "bar"),
            ],
        );

        cluster_actor.replicate(&mut test_logger, heartbeat).await;

        // WHEN - commit until 2
        let handler = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                match message {
                    CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, value) } => {
                        assert_eq!(value, "bar");
                        if key == "foo2" {
                            break;
                        }
                    }
                    _ => continue,
                }
            }
        });
        let heartbeat = heartbeat_create_helper(0, 2, vec![]);
        cluster_actor.replicate(&mut test_logger, heartbeat).await;

        // THEN
        assert_eq!(cluster_actor.replication.commit_idx, 2);
        assert_eq!(test_logger.log_index, 2.into());
        handler.await.unwrap();
    }
}
