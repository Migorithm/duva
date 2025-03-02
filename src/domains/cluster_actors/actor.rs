use crate::domains::{
    append_only_files::{
        WriteOperation, WriteRequest, interfaces::TAof, log::LogIndex, logger::Logger,
    },
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
    pub(crate) consensus_tracker: ConsensusTracker,
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
            consensus_tracker: ConsensusTracker::default(),
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

    pub fn set_replication_info(&mut self, leader_repl_id: String, hwm: u64) {
        self.replication.leader_repl_id = leader_repl_id;
        self.replication.hwm = hwm;
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
        write_request: WriteRequest,
        sender: tokio::sync::oneshot::Sender<Option<LogIndex>>,
    ) -> anyhow::Result<()> {
        // Skip consensus for no replicas
        let append_entries: Vec<WriteOperation> =
            logger.create_log_entries(&write_request, self.get_lowest_hwm()).await?;

        let repl_count = self.followers().count();
        if repl_count == 0 {
            let _ = sender.send(None);
            return Ok(());
        }
        self.consensus_tracker.add(logger.log_index, sender, repl_count);

        let mut tasks = self
            .generate_follower_entries(append_entries)
            .map(|(peer, hb)| peer.write_io(hb))
            .collect::<FuturesUnordered<_>>();

        // ! SAFETY DO NOT inline tasks.next().await in the while loop
        while let Some(_) = tasks.next().await {}
        Ok(())
    }

    pub(crate) fn leader_mut(&mut self) -> Option<&mut Peer> {
        self.members.values_mut().find(|peer| matches!(peer.w_conn.kind, PeerKind::Leader))
    }

    pub(crate) fn followers(&self) -> impl Iterator<Item = (&PeerIdentifier, &Peer, u64)> {
        self.members.iter().filter_map(|(id, peer)| match peer.w_conn.kind {
            PeerKind::Follower(current_commit) => Some((id, peer, current_commit)),
            _ => None,
        })
    }

    pub(crate) fn followers_mut(&mut self) -> impl Iterator<Item = (&mut Peer, u64)> {
        self.members.values_mut().into_iter().filter_map(|peer| match peer.w_conn.kind {
            PeerKind::Follower(current_commit) => Some((peer, current_commit)),
            _ => None,
        })
    }

    /// create entries per follower.
    pub(crate) fn generate_follower_entries(
        &mut self,
        append_entries: Vec<WriteOperation>,
    ) -> impl Iterator<Item = (&mut Peer, HeartBeatMessage)> {
        let default_heartbeat: HeartBeatMessage = self.replication.default_heartbeat(0);
        self.followers_mut().map(move |(peer, hwm)| {
            let logs =
                append_entries.iter().filter(|op| *op.log_index > hwm).cloned().collect::<Vec<_>>();
            (peer, default_heartbeat.clone().set_append_entries(logs))
        })
    }

    pub(crate) fn get_lowest_hwm(&self) -> u64 {
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
            if let Some(mut consensus) = self.consensus_tracker.take(&offset) {
                println!("[INFO] Received acks for log index num: {}", offset);
                consensus.apply_vote();

                if let Some(consensus) = consensus.maybe_not_finished(offset) {
                    self.consensus_tracker.insert(offset, consensus);
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
        self.replication.hwm += 1;
        let message: HeartBeatMessage = self.replication.default_heartbeat(0);
        println!("[INFO] Sending commit request on {}", message.hwm);

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
        if self.replication.hwm < heartbeat.hwm {
            println!("[INFO] Received commit offset {}", heartbeat.hwm);
            //* Retrieve the logs that fall between the current 'log' index of this node and leader 'commit' idx
            for log in logger.range(self.replication.hwm, heartbeat.hwm) {
                let _ = self.cache_manager.apply_log(log.request).await;
                self.replication.hwm = log.log_index.into();
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
#[allow(unused_variables)]
mod test {

    use tokio::{net::TcpStream, sync::mpsc::Receiver};

    use super::*;
    use crate::{
        adapters::aof::memory_aof::InMemoryAof,
        domains::{
            append_only_files::{WriteOperation, WriteRequest},
            caches::{cache_objects::CacheEntry, command::CacheCommand},
            cluster_actors::commands::ClusterCommand,
            peers::connected_types::Follower,
            saves::snapshot::snapshot_applier::SnapshotApplier,
        },
    };
    use std::{
        ops::Range,
        time::{Duration, SystemTime},
    };

    fn write_operation_create_helper(index_num: u64, key: &str, value: &str) -> WriteOperation {
        WriteOperation {
            log_index: index_num.into(),
            request: WriteRequest::Set { key: key.into(), value: value.into() },
        }
    }
    fn heartbeat_create_helper(
        term: u64,
        hwm: u64,
        op_logs: Vec<WriteOperation>,
    ) -> HeartBeatMessage {
        HeartBeatMessage {
            term,
            hwm,
            append_entries: op_logs,
            ban_list: vec![],
            heartbeat_from: PeerIdentifier::new("localhost", 8080),
            leader_replid: "localhost".to_string(),
            hop_count: 0,
        }
    }

    fn cluster_actor_create_helper() -> (ClusterActor, Receiver<CacheCommand>) {
        let (cache_handler, receiver) = tokio::sync::mpsc::channel(100);
        let cache_manager: CacheManager = CacheManager::test_new(cache_handler);
        let (tx, rx) = tokio::sync::watch::channel(false);
        let replication = ReplicationInfo::new(None, "localhost", 8080);
        (ClusterActor::new(100, replication, cache_manager, tx), receiver)
    }

    async fn cluster_member_create_helper(
        actor: &mut ClusterActor,
        num_stream: Range<u16>,
        cluster_sender: tokio::sync::mpsc::Sender<ClusterCommand>,

        current_follower_offset: u64,
    ) {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();

        for port in num_stream {
            actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new::<Follower>(
                    TcpStream::connect(bind_addr).await.unwrap(),
                    PeerKind::Follower(current_follower_offset),
                    cluster_sender.clone(),
                    PeerIdentifier::new("localhost", port),
                    SnapshotApplier::new(actor.cache_manager.clone(), SystemTime::now()),
                ),
            );
        }
    }

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

    #[tokio::test]
    async fn leader_consensus_tracker_not_changed_when_followers_not_exist() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());
        let mut cluster_actor = cluster_actor_create_helper().0;
        let (tx, rx) = tokio::sync::oneshot::channel();

        // WHEN
        cluster_actor
            .req_consensus(
                &mut test_logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                tx,
            )
            .await
            .unwrap();

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(test_logger.log_index, 1.into());
    }

    #[tokio::test]
    async fn req_consensus_inserts_consensus_voting() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());
        let mut cluster_actor = cluster_actor_create_helper().0;

        // - add 5 followers
        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        cluster_member_create_helper(&mut cluster_actor, 0..5, cluster_sender, 0).await;

        let (tx, _) = tokio::sync::oneshot::channel();

        // WHEN
        cluster_actor
            .req_consensus(
                &mut test_logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                tx,
            )
            .await
            .unwrap();

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(test_logger.log_index, 1.into());
    }

    #[tokio::test]
    async fn apply_acks_delete_consensus_voting_when_consensus_reached() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());
        let mut cluster_actor = cluster_actor_create_helper().0;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        // - add followers to create quorum
        cluster_member_create_helper(&mut cluster_actor, 0..4, cluster_sender, 0).await;
        let (client_request_sender, client_wait) = tokio::sync::oneshot::channel();
        cluster_actor
            .req_consensus(
                &mut test_logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                client_request_sender,
            )
            .await
            .unwrap();

        // WHEN
        cluster_actor.apply_acks(vec![1.into()]);
        cluster_actor.apply_acks(vec![1.into()]);

        // up to this point, tracker hold the consensus
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);

        // ! Majority votes made
        cluster_actor.apply_acks(vec![1.into()]);

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(test_logger.log_index, 1.into());
        client_wait.await.unwrap();
    }

    #[tokio::test]
    async fn logger_create_entries_from_lowest() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());

        let test_logs = vec![
            write_operation_create_helper(1, "foo", "bar"),
            write_operation_create_helper(2, "foo2", "bar"),
            write_operation_create_helper(3, "foo3", "bar"),
        ];
        test_logger.write_log_entries(test_logs.clone()).await.unwrap();

        // WHEN
        const LOWEST_FOLLOWER_COMMIT_INDEX: u64 = 2;
        let logs = test_logger
            .create_log_entries(
                &WriteRequest::Set { key: "foo4".into(), value: "bar".into() },
                LOWEST_FOLLOWER_COMMIT_INDEX,
            )
            .await
            .unwrap();

        // THEN
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 3.into());
        assert_eq!(logs[1].log_index, 4.into());
        assert_eq!(test_logger.log_index, 4.into());
    }

    #[tokio::test]
    async fn generate_follower_entries() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());

        let mut cluster_actor = cluster_actor_create_helper().0;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        cluster_member_create_helper(&mut cluster_actor, 0..5, cluster_sender.clone(), 3).await;

        let test_logs = vec![
            write_operation_create_helper(1, "foo", "bar"),
            write_operation_create_helper(2, "foo2", "bar"),
            write_operation_create_helper(3, "foo3", "bar"),
        ];

        cluster_actor.replication.hwm = 3;

        test_logger.write_log_entries(test_logs).await.unwrap();

        //WHEN
        // *add lagged followers with its commit index being 1
        cluster_member_create_helper(&mut cluster_actor, 5..7, cluster_sender, 1).await;

        // * add new log - this must create entries that are greater than 3
        let lowest_hwm = cluster_actor.get_lowest_hwm();
        let append_entries = test_logger
            .create_log_entries(
                &WriteRequest::Set { key: "foo4".into(), value: "bar".into() },
                lowest_hwm,
            )
            .await
            .unwrap();

        // THEN
        assert_eq!(append_entries.len(), 3);

        let entries = cluster_actor.generate_follower_entries(append_entries).collect::<Vec<_>>();

        // * for old followers must have 1 entry
        assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 1).count(), 5);
        // * for lagged followers must have 3 entries
        assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 3).count(), 2)
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_log() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());
        let mut cluster_actor = cluster_actor_create_helper().0;
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
        assert_eq!(cluster_actor.replication.hwm, 0);
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
        let (mut cluster_actor, mut cache_actor) = cluster_actor_create_helper();

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
        let task = tokio::spawn(async move {
            while let Some(message) = cache_actor.recv().await {
                match message {
                    CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, value) } => {
                        assert_eq!(value, "bar");
                        if key == "foo2" {
                            break;
                        }
                    },
                    _ => continue,
                }
            }
        });
        let heartbeat = heartbeat_create_helper(0, 2, vec![]);
        cluster_actor.replicate(&mut test_logger, heartbeat).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm, 2);
        assert_eq!(test_logger.log_index, 2.into());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state_only_upto_hwm() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryAof::default());

        let (mut cluster_actor, mut rx) = cluster_actor_create_helper();

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
        let task = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                match message {
                    CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, value) } => {
                        assert_eq!(value, "bar");
                        if key == "foo2" {
                            break;
                        }
                    },
                    _ => continue,
                }
            }
        });
        const hwm: u64 = 1;
        let heartbeat = heartbeat_create_helper(0, hwm, vec![]);
        cluster_actor.replicate(&mut test_logger, heartbeat).await;

        // THEN
        assert!(tokio::time::timeout(Duration::from_secs(1), task).await.is_err());
        assert_eq!(cluster_actor.replication.hwm, 1);
        assert_eq!(test_logger.log_index, 2.into());
    }
}
