use super::commands::AddPeer;
use super::commands::RequestVote;
use super::commands::RequestVoteReply;
use super::commands::WriteConsensusResponse;
use super::heartbeats::heartbeat::AppendEntriesRPC;
use super::heartbeats::heartbeat::ClusterHeartBeat;
use super::heartbeats::scheduler::HeartBeatScheduler;
use super::replication::HeartBeatMessage;
use super::replication::ReplicationId;
use super::replication::ReplicationState;
use super::replication::time_in_secs;
use super::{commands::ClusterCommand, replication::BannedPeer, *};
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::WriteRequest;
use crate::domains::append_only_files::interfaces::TWriteAheadLog;
use crate::domains::append_only_files::logger::ReplicatedLogs;
use crate::domains::cluster_actors::election_state::ElectionState;
use crate::domains::{caches::cache_manager::CacheManager, query_parsers::QueryIO};

#[derive(Debug)]
pub struct ClusterActor {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationState,
    pub(crate) node_timeout: u128,
    pub(crate) consensus_tracker: LogConsensusTracker,
    pub(crate) receiver: tokio::sync::mpsc::Receiver<ClusterCommand>,
    pub(crate) self_handler: tokio::sync::mpsc::Sender<ClusterCommand>,
    pub(crate) heartbeat_scheduler: HeartBeatScheduler,
}

impl ClusterActor {
    pub(crate) fn new(
        node_timeout: u128,
        init_repl_info: ReplicationState,
        heartbeat_interval_in_mills: u64,
    ) -> Self {
        let (self_handler, receiver) = tokio::sync::mpsc::channel(100);
        let heartbeat_scheduler = HeartBeatScheduler::run(
            self_handler.clone(),
            init_repl_info.is_leader_mode,
            heartbeat_interval_in_mills,
        );

        Self {
            heartbeat_scheduler,
            replication: init_repl_info,
            node_timeout,
            receiver,
            self_handler,
            members: BTreeMap::new(),
            consensus_tracker: LogConsensusTracker::default(),
        }
    }

    pub(crate) fn hop_count(fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }

    pub(crate) fn replicas(&self) -> impl Iterator<Item = (&PeerIdentifier, &Peer, u64)> {
        self.members.iter().filter_map(|(id, peer)| match &peer.kind {
            PeerKind::Replica { watermark: hwm, replid: leader_repl_id } => Some((id, peer, *hwm)),
            _ => None,
        })
    }

    pub(crate) fn replicas_mut(&mut self) -> impl Iterator<Item = (&mut Peer, u64)> {
        self.members.values_mut().into_iter().filter_map(|peer| match peer.kind.clone() {
            PeerKind::Replica { watermark: hwm, replid: leader_repl_id } => Some((peer, hwm)),
            _ => None,
        })
    }

    fn find_replica_mut(&mut self, peer_id: &PeerIdentifier) -> Option<&mut Peer> {
        self.members.get_mut(peer_id).filter(|peer| matches!(peer.kind, PeerKind::Replica { .. }))
    }

    pub(crate) async fn send_cluster_heartbeat(
        &mut self,
        hop_count: u8,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        // TODO randomly choose the peer to send the message
        let msg = ClusterHeartBeat(
            self.replication
                .default_heartbeat(hop_count, logger.log_index, logger.term)
                .set_cluster_nodes(self.cluster_nodes()),
        );

        for peer in self.members.values_mut() {
            let _ = peer.write_io(msg.clone()).await;
        }
    }

    pub(crate) async fn add_peer(&mut self, add_peer_cmd: AddPeer) {
        let AddPeer { peer_id: peer_addr, peer } = add_peer_cmd;

        self.replication.remove_from_ban_list(&peer_addr);

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer_addr, peer) {
            existing_peer.kill().await;
        }
    }
    pub(crate) async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.remove(peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.kill().await;
            return Some(());
        }
        None
    }

    pub(crate) fn set_replication_info(&mut self, leader_repl_id: ReplicationId, hwm: u64) {
        self.replication.replid = leader_repl_id;
        self.replication.hwm = hwm;
    }

    /// Remove the peers that are idle for more than ttl_mills
    pub(crate) async fn remove_idle_peers(&mut self) {
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

    pub(crate) async fn gossip(
        &mut self,
        hop_count: u8,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        // If hop_count is 0, don't send the message to other peers
        if hop_count == 0 {
            return;
        };
        let hop_count = hop_count - 1;
        self.send_cluster_heartbeat(hop_count, logger).await;
    }

    pub(crate) async fn forget_peer(
        &mut self,
        peer_addr: PeerIdentifier,
    ) -> anyhow::Result<Option<()>> {
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
    fn retain_only_recent_banned_nodes(&mut self) {
        // remove the nodes that are banned for more than 60 seconds
        let current_time_in_sec = time_in_secs().unwrap();
        self.replication.ban_list.retain(|node| current_time_in_sec - node.ban_time < 60);
    }
    async fn remove_banned_peers(&mut self) {
        let ban_list = self.replication.ban_list.clone();
        for banned_peer in ban_list {
            let _ = self.remove_peer(&banned_peer.p_id).await;
        }
    }

    pub(crate) async fn apply_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        self.merge_ban_list(ban_list);
        self.retain_only_recent_banned_nodes();
        self.remove_banned_peers().await;
    }

    pub(crate) fn update_on_hertbeat_message(&mut self, heartheat: &HeartBeatMessage) {
        if let Some(peer) = self.members.get_mut(&heartheat.heartbeat_from) {
            peer.last_seen = Instant::now();

            if let PeerKind::Replica { watermark: hwm, .. } = &mut peer.kind {
                *hwm = heartheat.hwm;
            }
        }
    }
    pub(crate) async fn try_create_append_entries(
        &mut self,
        logger: &mut ReplicatedLogs<impl TWriteAheadLog>,
        log: &WriteRequest,
    ) -> Result<Vec<WriteOperation>, WriteConsensusResponse> {
        if !self.replication.is_leader_mode {
            return Err(WriteConsensusResponse::Err("Write given to follower".into()));
        }

        let Ok(append_entries) =
            logger.create_log_entries(&log, self.take_low_watermark(), self.replication.term).await
        else {
            return Err(WriteConsensusResponse::Err("Write operation failed".into()));
        };

        // Skip consensus for no replicas
        if self.replicas().count() == 0 {
            return Err(WriteConsensusResponse::LogIndex(Some(logger.log_index)));
        }

        Ok(append_entries)
    }

    pub(crate) async fn req_consensus(
        &mut self,
        logger: &mut ReplicatedLogs<impl TWriteAheadLog>,
        log: WriteRequest,
        callback: tokio::sync::oneshot::Sender<WriteConsensusResponse>,
    ) -> anyhow::Result<()> {
        let (prev_log_index, prev_term) = (logger.log_index, logger.term);
        let append_entries = match self.try_create_append_entries(logger, &log).await {
            Ok(entries) => entries,
            Err(err) => {
                let _ = callback.send(err);
                return Ok(());
            },
        };

        self.consensus_tracker.add(logger.log_index, callback, self.replicas().count());
        self.generate_follower_entries(append_entries, prev_log_index, prev_term)
            .map(|(peer, hb)| peer.write_io(AppendEntriesRPC(hb)))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;

        Ok(())
    }

    pub(crate) async fn install_leader_state(
        &mut self,
        logs: Vec<WriteOperation>,
        cache_manager: &CacheManager,
    ) {
        println!("[INFO] Received Leader State - length {}", logs.len());
        for log in logs {
            let _ = cache_manager.apply_log(log.request).await;
            self.replication.hwm = log.log_index.into();
        }
    }

    /// create entries per follower.
    pub(crate) fn generate_follower_entries(
        &mut self,
        append_entries: Vec<WriteOperation>,
        prev_log_index: u64,
        prev_term: u64,
    ) -> impl Iterator<Item = (&mut Peer, HeartBeatMessage)> {
        let default_heartbeat: HeartBeatMessage =
            self.replication.default_heartbeat(0, prev_log_index, prev_term);
        self.replicas_mut().map(move |(peer, hwm)| {
            let logs =
                append_entries.iter().filter(|op| op.log_index > hwm).cloned().collect::<Vec<_>>();
            (peer, default_heartbeat.clone().set_append_entries(logs))
        })
    }

    pub(crate) fn take_low_watermark(&self) -> Option<u64> {
        self.members
            .values()
            .into_iter()
            .filter_map(|peer| match &peer.kind {
                PeerKind::Replica { watermark, replid: leader_repl_id } => Some(*watermark),
                _ => None,
            })
            .min()
    }

    pub(crate) fn apply_acks(&mut self, offsets: Vec<u64>) {
        offsets.into_iter().for_each(|offset| {
            if let Some(mut consensus) = self.consensus_tracker.take(&offset) {
                println!("[INFO] Received acks for log index num: {}", offset);
                consensus.increase_vote();

                if let Some(consensus) = consensus.maybe_not_finished(offset) {
                    self.consensus_tracker.insert(offset, consensus);
                }
            }
        });
    }

    // After send_ack: Leader updates its knowledge of follower's progress
    async fn send_ack(&mut self, send_to: &PeerIdentifier, log_index: u64) {
        if let Some(leader) = self.members.get_mut(send_to) {
            // TODO send the last offset instead of multiple offsets.
            let _ = leader.write_io(QueryIO::AppendEntriesResponse(vec![log_index])).await;
        }
    }

    // After send_negative_ack: Leader needs to backtrack and send earlier entries
    async fn send_negative_ack(&self, send_to: &PeerIdentifier, prev_log_index: u64) {
        todo!()
    }

    pub(crate) async fn send_commit_heartbeat(&mut self, offset: u64) {
        // TODO is there any case where I can use offset input?
        self.replication.hwm += 1;
        let message: HeartBeatMessage =
            self.replication.default_heartbeat(0, offset, self.replication.term);
        println!("[INFO] Sending commit request on {}", message.hwm);
        self.send_to_replicas(AppendEntriesRPC(message)).await;
    }

    pub(crate) async fn replicate(
        &mut self,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        mut heartbeat: HeartBeatMessage,
        cache_manager: &CacheManager,
    ) {
        // * logging case
        if let Err(_) = self.try_append_entries(wal, &mut heartbeat).await {
            return;
        };

        // * state machine case
        self.change_state(heartbeat.hwm, wal, cache_manager).await;
    }

    async fn try_append_entries(
        &mut self,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        rpc: &mut HeartBeatMessage,
    ) -> anyhow::Result<()> {
        if rpc.append_entries.is_empty() {
            return Ok(());
        }

        // If rpc.prev_log_index == 0, skip the consistency check entirely.

        if let Err(e) =
            self.check_previous_entry_consistency(wal, rpc.prev_log_index, rpc.prev_log_term).await
        {
            self.send_negative_ack(&rpc.heartbeat_from, wal.log_index).await;
            return Err(e);
        }

        match wal.write_log_entries(std::mem::take(&mut rpc.append_entries)).await {
            Ok(match_index) => {
                self.send_ack(&rpc.heartbeat_from, match_index).await;
                Ok(())
            },
            Err(e) => {
                println!("[ERROR] Failed to write log entries: {:?}", e);
                Err(e)
            },
        }
    }

    async fn check_previous_entry_consistency(
        &self,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        prev_log_index: u64,
        prev_log_term: u64,
    ) -> anyhow::Result<()> {
        // ! Consistency Check Issue:
        //     If the leader sends an AppendEntries RPC with prev_log_index > 0,
        //     but the follower's WAL is empty, there's no way the follower can verify the consistency of the previous log entry (since it doesn't exist).
        // ! First Entry Handling:
        //     A new follower or one that has just cleared its logs will have an empty WAL.
        //     In this case, it should only accept entries with prev_log_index = 0 (which indicates the beginning of the log).
        if wal.is_empty() {
            if prev_log_index == 0 {
                return Ok(());
            }
            return Err(anyhow::anyhow!("WAL is empty and prev_log_index > 0"));
        }

        // Check if the previous log index is within our log range
        let log_start_index = wal.log_start_index();

        if prev_log_index < log_start_index {
            // Previous log index is too old, we've compacted past it
            return Err(anyhow::anyhow!("Previous log index is too old"));
        }

        // TODO If entry_matches is false:
        // * Raft followers should truncate their log starting at prev_log_index + 1 and then append the new entries
        // * Just returning an error is breaking consistency
        let entry_matches = wal
            .read_at(prev_log_index)
            .await
            .map(|entry| entry.term == prev_log_term)
            .unwrap_or(false);

        if !entry_matches {
            // Log inconsistency
            return Err(anyhow::anyhow!("Log inconsistency"));
        }

        Ok(())
    }

    pub(crate) async fn send_leader_heartbeat(
        &mut self,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        self.send_to_replicas(AppendEntriesRPC(self.replication.default_heartbeat(
            0,
            logger.log_index,
            logger.term,
        )))
        .await;
    }

    async fn send_to_replicas(&mut self, msg: impl Into<QueryIO> + Send + Clone) {
        self.replicas_mut()
            .into_iter()
            .map(|(peer, _)| peer.write_io(msg.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) fn cluster_nodes(&self) -> Vec<String> {
        self.members
            .values()
            .into_iter()
            .map(|peer| match &peer.kind {
                PeerKind::Replica { watermark: hwm, replid } => {
                    format!("{} follower {}", peer.addr, replid)
                },
                PeerKind::NonDataPeer { replid } => {
                    format!("{} follower {}", peer.addr, replid)
                },
            })
            .chain(std::iter::once(self.replication.self_info()))
            .collect()
    }

    pub(crate) async fn run_for_election(&mut self, last_log_index: u64, last_log_term: u64) {
        let ElectionState::Follower { voted_for: None } = self.replication.election_state else {
            return;
        };

        self.replication.become_candidate(self.replicas().count());
        let request_vote = RequestVote::new(&self.replication, last_log_index, last_log_term);

        println!("[INFO] Running for election term {}", self.replication.term);
        self.replicas_mut()
            .map(|(peer, _)| peer.write_io(request_vote.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) async fn vote_election(&mut self, request_vote: RequestVote, current_log_idx: u64) {
        let grant_vote = current_log_idx <= request_vote.last_log_index
            && self.replication.may_become_follower(&request_vote.candidate_id, request_vote.term);

        println!(
            "[INFO] Voting for {} with term {} and granted: {}",
            request_vote.candidate_id, request_vote.term, grant_vote
        );

        let term = self.replication.term;
        let Some(peer) = self.find_replica_mut(&request_vote.candidate_id) else {
            return;
        };
        let _ = peer.write_io(RequestVoteReply { term, vote_granted: grant_vote }).await;
    }

    pub(crate) async fn tally_vote(
        &mut self,
        request_vote_reply: RequestVoteReply,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        if !self.replication.should_become_leader(request_vote_reply.vote_granted) {
            return;
        }

        if self.replication.is_leader_mode {
            self.heartbeat_scheduler.switch().await;
        }
        let msg = self.replication.default_heartbeat(0, logger.log_index, logger.term);

        self.replicas_mut()
            .map(|(peer, _)| peer.write_io(AppendEntriesRPC(msg.clone())))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;

        //TODO - how to notify other followers of this election? What's the rule?
    }

    pub(crate) fn reset_election_timeout(&mut self, leader_id: &PeerIdentifier) {
        if let Some(peer) = self.members.get_mut(leader_id) {
            peer.last_seen = Instant::now();
        }
        self.heartbeat_scheduler.reset_election_timeout();
    }

    async fn change_state(
        &mut self,
        heartbeat_hwm: u64,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        cache_manager: &CacheManager,
    ) {
        if heartbeat_hwm > self.replication.hwm {
            println!("[INFO] Received commit offset {}", heartbeat_hwm);

            let old_hwm = self.replication.hwm;
            self.replication.hwm = std::cmp::min(heartbeat_hwm, wal.log_index);

            // Apply all newly committed entries to state machine
            for log_index in (old_hwm + 1)..=self.replication.hwm {
                if let Some(log) = wal.read_at(log_index).await {
                    match cache_manager.apply_log(log.request).await {
                        Ok(_) => {},
                        Err(e) => println!("[ERROR] Failed to apply log: {:?}", e),
                    }
                }
            }
        }
    }

    // TODO the node should step down to follower state if it’s a leader or candidate (Raft rule).
    pub(crate) fn apply_term_then_may_stepdown(
        &mut self,
        new_term: u64,
        heartbeat_from: &PeerIdentifier,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        if new_term > self.replication.term {
            self.replication.term = new_term;
            wal.term = new_term;
        }
    }
}

#[cfg(test)]
#[allow(unused_variables)]
mod test {
    use super::*;
    use crate::adapters::wal::memory_wal::InMemoryWAL;
    use crate::domains::append_only_files::WriteOperation;
    use crate::domains::append_only_files::WriteRequest;
    use crate::domains::caches::actor::CacheCommandSender;
    use crate::domains::caches::cache_objects::CacheEntry;
    use crate::domains::caches::command::CacheCommand;
    use crate::domains::cluster_actors::commands::ClusterCommand;
    use crate::presentation::clusters::listeners::create_peer;

    use std::ops::Range;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::channel;

    fn write_operation_create_helper(index_num: u64, key: &str, value: &str) -> WriteOperation {
        WriteOperation {
            log_index: index_num.into(),
            request: WriteRequest::Set { key: key.into(), value: value.into() },
            term: 0,
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
            prev_log_index: if !op_logs.is_empty() { (op_logs[0].log_index - 1) } else { 0 },
            prev_log_term: 0,
            append_entries: op_logs,
            ban_list: vec![],
            heartbeat_from: PeerIdentifier::new("localhost", 8080),
            replid: ReplicationId::Key("localhost".to_string().into()),
            hop_count: 0,
            cluster_nodes: vec![],
        }
    }

    fn cluster_actor_create_helper() -> ClusterActor {
        let replication = ReplicationState::new(None, "localhost", 8080);
        ClusterActor::new(100, replication, 100)
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
            let key = PeerIdentifier::new("localhost", port);
            actor.members.insert(
                PeerIdentifier::new("localhost", port),
                create_peer(
                    key.to_string(),
                    TcpStream::connect(bind_addr).await.unwrap(),
                    PeerKind::Replica {
                        watermark: follower_hwm,
                        replid: ReplicationId::Key("localhost".to_string().into()),
                    },
                    cluster_sender.clone(),
                ),
            );
        }
    }

    #[tokio::test]
    async fn test_hop_count_when_one() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::hop_count(fanout, 1);
        // THEN
        assert_eq!(hop_count, 0);
    }

    #[tokio::test]
    async fn test_hop_count_when_two() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::hop_count(fanout, 2);
        // THEN
        assert_eq!(hop_count, 0);
    }

    #[tokio::test]
    async fn test_hop_count_when_three() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::hop_count(fanout, 3);
        // THEN
        assert_eq!(hop_count, 1);
    }

    #[tokio::test]
    async fn test_hop_count_when_thirty() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::hop_count(fanout, 30);
        // THEN
        assert_eq!(hop_count, 4);
    }

    #[tokio::test]
    async fn leader_consensus_tracker_not_changed_when_followers_not_exist() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);
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
        assert_eq!(test_logger.log_index, 1);
    }

    #[tokio::test]
    async fn req_consensus_inserts_consensus_voting() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);

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
        assert_eq!(test_logger.log_index, 1);
    }

    #[tokio::test]
    async fn apply_acks_delete_consensus_voting_when_consensus_reached() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);
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
        cluster_actor.apply_acks(vec![1]);
        cluster_actor.apply_acks(vec![1]);

        // up to this point, tracker hold the consensus
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);

        // ! Majority votes made
        cluster_actor.apply_acks(vec![1]);

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(test_logger.log_index, 1);
        client_wait.await.unwrap();
    }

    #[tokio::test]
    async fn logger_create_entries_from_lowest() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);

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
                Some(LOWEST_FOLLOWER_COMMIT_INDEX),
                0,
            )
            .await
            .unwrap();

        // THEN
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 3);
        assert_eq!(logs[1].log_index, 4);
        assert_eq!(test_logger.log_index, 4);
    }

    #[tokio::test]
    async fn generate_follower_entries() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);

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
        let lowest_hwm = cluster_actor.take_low_watermark();
        let append_entries = test_logger
            .create_log_entries(
                &WriteRequest::Set { key: "foo4".into(), value: "bar".into() },
                lowest_hwm,
                0,
            )
            .await
            .unwrap();

        // THEN
        assert_eq!(append_entries.len(), 3);

        let entries =
            cluster_actor.generate_follower_entries(append_entries, 3, 0).collect::<Vec<_>>();

        // * for old followers must have 1 entry
        assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 1).count(), 5);
        // * for lagged followers must have 3 entries
        assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 3).count(), 2)
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_log() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);
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
        assert_eq!(test_logger.log_index, 2);
        let logs = test_logger.range(0, 2);
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 1);
        assert_eq!(logs[1].log_index, 2);
        assert_eq!(logs[0].request, WriteRequest::Set { key: "foo".into(), value: "bar".into() });
        assert_eq!(logs[1].request, WriteRequest::Set { key: "foo2".into(), value: "bar".into() });
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);
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
                    },
                    _ => continue,
                }
            }
        });
        let heartbeat = heartbeat_create_helper(0, 2, vec![]);
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm, 2);
        assert_eq!(test_logger.log_index, 2);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_multiple_committed_entries() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper();

        // Add multiple entries
        let entries = vec![
            write_operation_create_helper(1, "key1", "value1"),
            write_operation_create_helper(2, "key2", "value2"),
            write_operation_create_helper(3, "key3", "value3"),
        ];

        let heartbeat = heartbeat_create_helper(1, 0, entries);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        // First append entries but don't commit
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // Create a task to monitor applied entries
        let monitor_task = tokio::spawn(async move {
            let mut applied_keys = Vec::new();

            while let Some(message) = rx.recv().await {
                if let CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, _) } = message {
                    applied_keys.push(key);
                    if applied_keys.len() == 3 {
                        break;
                    }
                }
            }

            applied_keys
        });

        // WHEN - commit all entries
        let commit_heartbeat = heartbeat_create_helper(1, 3, vec![]);

        cluster_actor.replicate(&mut test_logger, commit_heartbeat, &cache_manager).await;

        // THEN
        // Verify that all entries were committed and applied in order
        let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
            .await
            .expect("Timeout waiting for entries to be applied")
            .expect("Task failed");

        assert_eq!(applied_keys, vec!["key1", "key2", "key3"]);
        assert_eq!(cluster_actor.replication.hwm, 3);
    }

    #[tokio::test]
    async fn test_partial_commit_with_new_entries() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper();

        // First, append some entries
        let first_entries = vec![
            write_operation_create_helper(1, "key1", "value1"),
            write_operation_create_helper(2, "key2", "value2"),
        ];

        let first_heartbeat = heartbeat_create_helper(1, 0, first_entries);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        cluster_actor.replicate(&mut test_logger, first_heartbeat, &cache_manager).await;

        // Create a task to monitor applied entries
        let monitor_task = tokio::spawn(async move {
            let mut applied_keys = Vec::new();

            while let Some(message) = rx.recv().await {
                if let CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, _) } = message {
                    applied_keys.push(key);
                    if applied_keys.len() == 1 {
                        break;
                    }
                }
            }

            applied_keys
        });

        // WHEN - commit partial entries and append new ones
        let second_entries = vec![write_operation_create_helper(3, "key3", "value3")];

        let second_heartbeat = heartbeat_create_helper(1, 1, second_entries);

        cluster_actor.replicate(&mut test_logger, second_heartbeat, &cache_manager).await;

        // THEN
        // Verify that only key1 was applied
        let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
            .await
            .expect("Timeout waiting for entries to be applied")
            .expect("Task failed");

        assert_eq!(applied_keys, vec!["key1"]);
        assert_eq!(cluster_actor.replication.hwm, 1);
        assert_eq!(test_logger.log_index, 3); // All entries are in the log
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state_only_upto_hwm() {
        // GIVEN
        let mut test_logger = ReplicatedLogs::new(InMemoryWAL::default(), 0, 0);

        let mut cluster_actor = cluster_actor_create_helper();

        // Log two entries but don't commit them yet
        let heartbeat = heartbeat_create_helper(
            0,
            0, // hwm=0, nothing committed yet
            vec![
                write_operation_create_helper(1, "foo", "bar"),
                write_operation_create_helper(2, "foo2", "bar"),
            ],
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        // This just appends the entries to the log but doesn't commit them
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // WHEN - commit only up to index 1
        let task = tokio::spawn(async move {
            let mut received_foo = false;

            while let Some(message) = rx.recv().await {
                match message {
                    CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, value) } => {
                        if key == "foo" {
                            received_foo = true;
                            assert_eq!(value, "bar");
                        } else if key == "foo2" {
                            // This should not happen in our test
                            panic!("foo2 should not be applied yet");
                        }
                    },
                    _ => continue,
                }
            }

            received_foo
        });

        // Send a heartbeat with hwm=1 to commit only the first entry
        const HWM: u64 = 1;
        let heartbeat = heartbeat_create_helper(0, HWM, vec![]);
        cluster_actor.replicate(&mut test_logger, heartbeat, &cache_manager).await;

        // THEN
        // Give the task a chance to process the message
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the task since we're done checking
        task.abort();

        // Verify state
        assert_eq!(cluster_actor.replication.hwm, 1);
        assert_eq!(test_logger.log_index, 2);
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
    //TODO Fix the following
    #[tokio::test]
    #[ignore]
    async fn test_cluster_nodes() {
        use tokio::net::TcpListener;
        // GIVEN

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();

        let mut cluster_actor = cluster_actor_create_helper();
        let self_identifier = ReplicationId::Key(bind_addr.to_string().into());

        // followers
        for port in [6379, 6380] {
            let key = PeerIdentifier::new("localhost", port);
            cluster_actor.members.insert(
                key.clone(),
                create_peer(
                    key.to_string(),
                    TcpStream::connect(bind_addr).await.unwrap(),
                    PeerKind::Replica { watermark: 0, replid: self_identifier.clone() },
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
            create_peer(
                (*second_shard_leader_identifier).clone(),
                TcpStream::connect(bind_addr).await.unwrap(),
                PeerKind::NonDataPeer {
                    replid: ReplicationId::Key(uuid::Uuid::now_v7().to_string()),
                },
                cluster_sender.clone(),
            ),
        );

        // follower for different shard
        for port in [2655, 2653] {
            let key = PeerIdentifier::new("localhost", port);
            cluster_actor.members.insert(
                key.clone(),
                create_peer(
                    key.to_string(),
                    TcpStream::connect(bind_addr_for_second_shard).await.unwrap(),
                    PeerKind::NonDataPeer {
                        replid: ReplicationId::Key((*second_shard_leader_identifier).clone()),
                    },
                    cluster_sender.clone(),
                ),
            );
        }

        // WHEN
        let res = dbg!(cluster_actor.cluster_nodes());

        assert_eq!(res.len(), 6);

        for value in [
            format!("127.0.0.1:6379 follower {}", self_identifier),
            format!("127.0.0.1:6380 follower {}", self_identifier),
            format!("{} leader - 0", second_shard_leader_identifier),
            format!("127.0.0.1:2655 follower {}", second_shard_leader_identifier),
            format!("127.0.0.1:2653 follower {}", second_shard_leader_identifier),
            format!("127.0.0.1:8080 myself,leader ? 0"),
        ] {
            assert!(res.contains(&value));
        }
    }
}
