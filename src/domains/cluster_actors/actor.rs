use super::{
    commands::{AddPeer, ClusterCommand},
    replication::{time_in_secs, BannedPeer, HeartBeatMessage, ReplicationInfo},
    *,
};
use crate::domains::{
    append_only_files::{
        interfaces::TWriteAheadLog, log::LogIndex, logger::Logger, WriteOperation, WriteRequest,
    },
    caches::cache_manager::CacheManager,
    query_parsers::QueryIO,
};
use std::time::Duration;
use tokio::{select, time::interval};

#[derive(Debug)]
pub struct ClusterActor {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationInfo,
    pub(crate) node_timeout: u128,
    pub(crate) consensus_tracker: ConsensusTracker,
    pub(crate) receiver: tokio::sync::mpsc::Receiver<ClusterCommand>,
    pub(crate) self_handler: tokio::sync::mpsc::Sender<ClusterCommand>,
    leader_mode_heartbeat_sender: Option<tokio::sync::oneshot::Sender<()>>,
    // TODO notifier will be used when election process is implemented?
    notifier: tokio::sync::watch::Sender<bool>,
}

impl ClusterActor {
    pub fn new(
        node_timeout: u128,
        init_repl_info: ReplicationInfo,
        notifier: tokio::sync::watch::Sender<bool>,
    ) -> Self {
        let (self_handler, receiver) = tokio::sync::mpsc::channel(100);
        Self {
            replication: init_repl_info,
            node_timeout,
            notifier,
            receiver,
            self_handler,

            members: BTreeMap::new(),
            leader_mode_heartbeat_sender: None,
            consensus_tracker: ConsensusTracker::default(),
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

    pub fn set_replication_info(&mut self, leader_repl_id: PeerIdentifier, hwm: u64) {
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

    pub fn update_on_hertbeat_message(&mut self, heartheat: &HeartBeatMessage) {
        if let Some(peer) = self.members.get_mut(&heartheat.heartbeat_from) {
            peer.last_seen = Instant::now();

            if let PeerKind::Follower { hwm, .. } = &mut peer.kind {
                *hwm = heartheat.hwm;
            }
        }
    }

    pub async fn update_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        self.merge_ban_list(ban_list);

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
        logger: &mut Logger<impl TWriteAheadLog>,
        write_request: WriteRequest,
        sender: tokio::sync::oneshot::Sender<Option<LogIndex>>,
    ) -> anyhow::Result<()> {
        // Skip consensus for no replicas
        let append_entries: Vec<WriteOperation> =
            logger.create_log_entries(&write_request, self.get_lowest_hwm()).await?;

        let repl_count = self.followers().count();
        if repl_count == 0 {
            let _ = sender.send(Some(logger.log_index));
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
        self.members.values_mut().find(|peer| matches!(peer.kind, PeerKind::Leader))
    }

    pub(crate) async fn install_leader_state(&mut self, logs: Vec<WriteOperation>, cache_manager: &CacheManager) {
        println!("[INFO] Received Leader State - length {}", logs.len());
        for log in logs {
            let _ = cache_manager.apply_log(log.request).await;
            self.replication.hwm = log.log_index.into();
        }
    }

    pub(crate) fn followers(&self) -> impl Iterator<Item=(&PeerIdentifier, &Peer, u64)> {
        self.members.iter().filter_map(|(id, peer)| match &peer.kind {
            PeerKind::Follower { hwm, leader_repl_id } => Some((id, peer, *hwm)),
            _ => None,
        })
    }

    pub(crate) fn followers_mut(&mut self) -> impl Iterator<Item=(&mut Peer, u64)> {
        self.members.values_mut().into_iter().filter_map(|peer| match peer.kind.clone() {
            PeerKind::Follower { hwm, leader_repl_id } => Some((peer, hwm)),
            _ => None,
        })
    }

    /// create entries per follower.
    pub(crate) fn generate_follower_entries(
        &mut self,
        append_entries: Vec<WriteOperation>,
    ) -> impl Iterator<Item=(&mut Peer, HeartBeatMessage)> {
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
            .filter_map(|peer| match &peer.kind {
                PeerKind::Follower { hwm, leader_repl_id } => Some(*hwm),
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
        println!(
            "[INFO] Sending commit offset {} to followers",
            self.replication.hwm
        );
        let message: HeartBeatMessage = self.replication.default_heartbeat(0);
        println!("[INFO] Sending commit request on {}", message.hwm);
        self.send_to_replicas(message).await;
    }

    pub(crate) async fn replicate(
        &mut self,
        logger: &mut Logger<impl TWriteAheadLog>,
        heartbeat: HeartBeatMessage,
        cache_manager: &CacheManager,
    ) {
        // * lagging case
        if self.replication.hwm < heartbeat.hwm {
            println!("[INFO] Received commit offset {}", heartbeat.hwm);
            //* Retrieve the logs that fall between the current 'log' index of this node and leader 'commit' idx
            for log in logger.range(self.replication.hwm, heartbeat.hwm) {
                let _ = cache_manager.apply_log(log.request).await;
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

    pub(crate) async fn send_leader_heartbeat(&mut self) {
        let heartbeat = self.replication.default_heartbeat(0);
        self.send_to_replicas(heartbeat).await;
    }

    async fn send_to_replicas(&mut self, msg: impl Into<QueryIO> + Send + Clone) {
        let mut tasks = self
            .followers_mut()
            .into_iter()
            .map(|(peer, _)| peer.write_io(msg.clone()))
            .collect::<FuturesUnordered<_>>();
        while let Some(_) = tasks.next().await {}
    }

    pub fn heartbeat_periodically(&self, heartbeat_interval: u64) {
        let actor_handler = self.self_handler.clone();
        tokio::spawn(async move {
            let mut heartbeat_interval = interval(Duration::from_millis(heartbeat_interval));
            loop {
                heartbeat_interval.tick().await;
                let _ = actor_handler.send(ClusterCommand::SendHeartBeat).await;
            }
        });
    }
    pub fn leader_heartbeat_periodically(&mut self) {
        const LEADER_HEARTBEAT_INTERVAL: u64 = 300;
        let is_leader_mode = self.replication.is_leader_mode();
        if !is_leader_mode {
            return;
        }
        let actor_handler = self.self_handler.clone();

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            select! {
                _ = rx => {},
                _ = async {
                    let mut heartbeat_interval = interval(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL));
                    loop {
                        heartbeat_interval.tick().await;
                        let _ = actor_handler.send(ClusterCommand::SendLeaderHeartBeat).await;
                    }
                } => {},
            }
        });

        self.leader_mode_heartbeat_sender = Some(tx);
    }

    pub fn cluster_nodes(&self) -> Vec<String> {
        self.members
            .iter()
            .map(|(key, value)| format!("{key} {}", value.kind))
            .chain(std::iter::once(self.replication.self_info()))
            .collect()
    }
}

#[cfg(test)]
#[allow(unused_variables)]
mod test {
    use tokio::{net::TcpStream, sync::mpsc::channel};

    use super::*;
    use crate::{
        adapters::wal::memory_wal::InMemoryWAL,
        domains::{
            append_only_files::{WriteOperation, WriteRequest},
            caches::{actor::CacheCommandSender, cache_objects::CacheEntry, command::CacheCommand},
            cluster_actors::commands::ClusterCommand,
            peers::connected_types::Follower,
        },
    };
    use std::{ops::Range, time::Duration};

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
            leader_replid: "localhost".to_string().into(),
            hop_count: 0,
        }
    }

    fn cluster_actor_create_helper() -> ClusterActor {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let replication = ReplicationInfo::new(None, "localhost", 8080);
        ClusterActor::new(100, replication, tx)
    }

    async fn cluster_member_create_helper(
        actor: &mut ClusterActor,
        num_stream: Range<u16>,
        cluster_sender: tokio::sync::mpsc::Sender<ClusterCommand>,
        cache_manager: CacheManager,
        follower_hwm: u64,
    ) {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();

        for port in num_stream {
            actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new::<Follower>(
                    TcpStream::connect(bind_addr).await.unwrap(),
                    PeerKind::Follower {
                        hwm: follower_hwm,
                        leader_repl_id: PeerIdentifier::new(
                            &actor.replication.self_host,
                            actor.replication.self_port,
                        ),
                    },
                    cluster_sender.clone(),
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

    #[tokio::test]
    async fn leader_consensus_tracker_not_changed_when_followers_not_exist() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryWAL::default());
        let mut cluster_actor = cluster_actor_create_helper();
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
        let mut test_logger = Logger::new(InMemoryWAL::default());

        let mut cluster_actor = cluster_actor_create_helper();

        // - add 5 followers
        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };

        cluster_member_create_helper(&mut cluster_actor, 0..5, cluster_sender, cache_manager, 0)
            .await;

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
        let mut test_logger = Logger::new(InMemoryWAL::default());
        let mut cluster_actor = cluster_actor_create_helper();

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        // - add followers to create quorum
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(&mut cluster_actor, 0..4, cluster_sender, cache_manager, 0)
            .await;
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
        let mut test_logger = Logger::new(InMemoryWAL::default());

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
        let mut test_logger = Logger::new(InMemoryWAL::default());

        let mut cluster_actor = cluster_actor_create_helper();

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(
            &mut cluster_actor,
            0..5,
            cluster_sender.clone(),
            cache_manager,
            3,
        )
            .await;

        let test_logs = vec![
            write_operation_create_helper(1, "foo", "bar"),
            write_operation_create_helper(2, "foo2", "bar"),
            write_operation_create_helper(3, "foo3", "bar"),
        ];

        cluster_actor.replication.hwm = 3;

        test_logger.write_log_entries(test_logs).await.unwrap();

        //WHEN
        // *add lagged followers with its commit index being 1
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(&mut cluster_actor, 5..7, cluster_sender, cache_manager, 1)
            .await;

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
        let mut test_logger = Logger::new(InMemoryWAL::default());
        let mut cluster_actor = cluster_actor_create_helper();
        // WHEN - term
        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, "foo", "bar"),
                write_operation_create_helper(2, "foo2", "bar"),
            ],
        );
        let cache_manager = CacheManager {
            inboxes: (0..10).map(|_| CacheCommandSender(channel(10).0)).collect::<Vec<_>>(),
        };
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

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
        let mut test_logger = Logger::new(InMemoryWAL::default());
        let (cache_handler, mut receiver) = tokio::sync::mpsc::channel(100);
        let mut cluster_actor = cluster_actor_create_helper();

        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, "foo", "bar"),
                write_operation_create_helper(2, "foo2", "bar"),
            ],
        );

        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(cache_handler)] };
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // WHEN - commit until 2
        let task = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
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
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm, 2);
        assert_eq!(test_logger.log_index, 2.into());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state_only_upto_hwm() {
        // GIVEN
        let mut test_logger = Logger::new(InMemoryWAL::default());

        let mut cluster_actor = cluster_actor_create_helper();

        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, "foo", "bar"),
                write_operation_create_helper(2, "foo2", "bar"),
            ],
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // WHEN - commit until 2
        let task = tokio::spawn(async move {
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
        const HWM: u64 = 1;
        let heartbeat = heartbeat_create_helper(0, HWM, vec![]);
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // THEN
        assert!(tokio::time::timeout(Duration::from_secs(1), task).await.is_err());
        assert_eq!(cluster_actor.replication.hwm, 1);
        assert_eq!(test_logger.log_index, 2.into());
    }

    /*
    cluster nodes should return the following:
    127.0.0.1:30004 follower 127.0.0.1:30001
    127.0.0.1:30002 leader - 5461-10922
    127.0.0.1:30003 leader - 10923-16383
    127.0.0.1:30005 follower 127.0.0.1:30002
    127.0.0.1:30006 follower 127.0.0.1:30003
    127.0.0.1:30001 myself,leader - 0-5460
    <ip:port> <flags> <leader> <link-state> <slot>
         */
    #[tokio::test]
    async fn test_cluster_nodes() {
        use tokio::net::TcpListener;
        // GIVEN

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();

        let mut cluster_actor = cluster_actor_create_helper();
        let self_identifier: PeerIdentifier = bind_addr.to_string().into();

        // followers
        for port in [6379, 6380] {
            cluster_actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new::<Follower>(
                    TcpStream::connect(bind_addr).await.unwrap(),
                    PeerKind::Follower { hwm: 0, leader_repl_id: self_identifier.clone() },
                    cluster_sender.clone(),
                ),
            );
        }

        let listener_for_second_shard = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr_for_second_shard = listener_for_second_shard.local_addr().unwrap();
        let second_shard_leader_identifier: PeerIdentifier =
            listener_for_second_shard.local_addr().unwrap().to_string().into();

        // leader for different shard?
        cluster_actor.members.insert(
            second_shard_leader_identifier.clone(),
            Peer::new::<Follower>(
                TcpStream::connect(bind_addr).await.unwrap(),
                PeerKind::PLeader,
                cluster_sender.clone(),
            ),
        );

        // follower for different shard
        for port in [2655, 2653] {
            cluster_actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new::<Follower>(
                    TcpStream::connect(bind_addr_for_second_shard).await.unwrap(),
                    PeerKind::PFollower { leader_repl_id: second_shard_leader_identifier.clone() },
                    cluster_sender.clone(),
                ),
            );
        }

        // WHEN
        let res = cluster_actor.cluster_nodes();

        assert_eq!(res.len(), 6);

        for value in [
            format!("127.0.0.1:6379 follower {}", self_identifier),
            format!("127.0.0.1:6380 follower {}", self_identifier),
            format!("{} leader - 0", second_shard_leader_identifier),
            format!("127.0.0.1:2655 follower {}", second_shard_leader_identifier),
            format!("127.0.0.1:2653 follower {}", second_shard_leader_identifier),
            format!("127.0.0.1:8080 myself,leader - 0"),
        ] {
            assert!(res.contains(&value));
        }
    }
}
