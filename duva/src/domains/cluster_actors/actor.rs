use super::commands::AddPeer;
use super::commands::ClusterCommand;
use super::commands::ConsensusClientResponse;
use super::commands::RejectionReason;
use super::commands::ReplicationResponse;
use super::commands::RequestVote;
use super::commands::RequestVoteReply;
use super::heartbeats::heartbeat::AppendEntriesRPC;
use super::heartbeats::heartbeat::ClusterHeartBeat;
use super::heartbeats::scheduler::HeartBeatScheduler;
use super::peer_connections::inbound::stream::InboundStream;
use super::peer_connections::outbound::stream::OutboundStream;
use super::replication::BannedPeer;
use super::replication::HeartBeatMessage;
use super::replication::ReplicationId;
use super::replication::ReplicationRole;
use super::replication::ReplicationState;
use super::replication::time_in_secs;
use super::session::ClientSessions;
use super::session::SessionRequest;
use super::*;
use crate::domains::cluster_actors::consensus::ElectionState;
use crate::domains::operation_logs::WriteOperation;
use crate::domains::operation_logs::WriteRequest;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::cluster_peer::ClusterNode;

use crate::domains::peers::peer::NodeKind;
use crate::domains::{caches::cache_manager::CacheManager, query_parsers::QueryIO};
use anyhow::Context;
use std::collections::VecDeque;
use std::iter;
use std::sync::atomic::Ordering;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct ClusterActor {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationState,
    pub(crate) node_timeout: u128,
    pub(crate) consensus_tracker: LogConsensusTracker,
    pub(crate) receiver: tokio::sync::mpsc::Receiver<ClusterCommand>,
    pub(crate) self_handler: tokio::sync::mpsc::Sender<ClusterCommand>,
    pub(crate) heartbeat_scheduler: HeartBeatScheduler,
    pub(crate) topology_writer: tokio::fs::File,
    pub(crate) node_change_broadcast: tokio::sync::broadcast::Sender<Vec<PeerIdentifier>>,
}

impl ClusterActor {
    pub(crate) fn new(
        node_timeout: u128,
        init_repl_info: ReplicationState,
        heartbeat_interval_in_mills: u64,
        topology_writer: File,
    ) -> Self {
        let (self_handler, receiver) = tokio::sync::mpsc::channel(100);
        let heartbeat_scheduler = HeartBeatScheduler::run(
            self_handler.clone(),
            init_repl_info.is_leader_mode,
            heartbeat_interval_in_mills,
        );

        let (tx, _) = tokio::sync::broadcast::channel::<Vec<PeerIdentifier>>(100);

        Self {
            heartbeat_scheduler,
            replication: init_repl_info,
            node_timeout,
            receiver,
            self_handler,
            members: BTreeMap::new(),
            consensus_tracker: LogConsensusTracker::default(),
            topology_writer,
            node_change_broadcast: tx,
        }
    }

    pub(crate) fn hop_count(fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }

    pub(crate) fn replicas(&self) -> impl Iterator<Item = (&PeerIdentifier, &Peer, u64)> {
        self.members.iter().filter_map(|(id, peer)| {
            (peer.kind() == &NodeKind::Replica).then_some((id, peer, peer.match_index()))
        })
    }

    pub(crate) fn replicas_mut(&mut self) -> impl Iterator<Item = (&mut Peer, u64)> {
        self.members.values_mut().filter_map(|peer| {
            let match_index = peer.match_index();
            (peer.kind() == &NodeKind::Replica).then_some((peer, match_index))
        })
    }

    fn find_replica_mut(&mut self, peer_id: &PeerIdentifier) -> Option<&mut Peer> {
        self.members.get_mut(peer_id).filter(|peer| peer.kind() == &NodeKind::Replica)
    }

    pub(crate) async fn send_cluster_heartbeat(
        &mut self,
        hop_count: u8,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        // TODO randomly choose the peer to send the message
        let msg = ClusterHeartBeat(
            self.replication
                .default_heartbeat(hop_count, logger.last_log_index, logger.last_log_term)
                .set_cluster_nodes(self.cluster_nodes()),
        );

        for peer in self.members.values_mut() {
            let _ = peer.send_to_peer(msg.clone()).await;
        }
    }

    pub(crate) async fn snapshot_topology(&mut self) -> anyhow::Result<()> {
        // TODO: consider single writer access to file
        let topology = self
            .cluster_nodes()
            .into_iter()
            .map(|cn| cn.to_string())
            .collect::<Vec<_>>()
            .join("\r\n");
        self.topology_writer.seek(std::io::SeekFrom::Start(0)).await?;
        self.topology_writer.set_len(topology.len() as u64).await?;
        self.topology_writer.write_all(topology.as_bytes()).await?;
        Ok(())
    }

    async fn add_peer(&mut self, add_peer_cmd: AddPeer) {
        let AddPeer { peer_id, peer } = add_peer_cmd;

        self.replication.remove_from_ban_list(&peer_id);

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer_id, peer) {
            existing_peer.kill().await;
        }

        self.node_change_broadcast
            .send(
                self.members
                    .keys()
                    .cloned()
                    .chain(iter::once(self.replication.self_identifier()))
                    .collect(),
            )
            .ok();
    }
    pub(crate) async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.remove(peer_addr) {
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.kill().await;
            return Some(());
        }
        None
    }

    pub(crate) async fn discover_cluster(
        &mut self,
        connect_to: PeerIdentifier,
    ) -> anyhow::Result<()> {
        let mut queue = VecDeque::from(vec![connect_to.clone()]);
        while let Some(connect_to) = queue.pop_front() {
            queue.extend(self.connect_to_server(connect_to).await?);
        }
        Ok(())
    }

    async fn connect_to_server(
        &mut self,
        connect_to: PeerIdentifier,
    ) -> anyhow::Result<Vec<PeerIdentifier>> {
        if self.members.contains_key(&connect_to) {
            return Ok(vec![]);
        }
        let stream = OutboundStream::new(connect_to, self.replication.clone())
            .await?
            .make_handshake(self.replication.self_port)
            .await?;

        if stream.my_repl_info.replid == ReplicationId::Undecided {
            let connected_node_info = stream
                .connected_node_info
                .as_ref()
                .context("Connected node info not found. Cannot set replication id")?;
            self.set_replication_info(connected_node_info.replid.clone(), connected_node_info.hwm);
        }

        let (add_peer, peer_list) = stream.create_peer_cmd(self.self_handler.clone())?;
        self.add_peer(add_peer).await;
        Ok(peer_list)
    }

    pub(crate) async fn accept_inbound_stream(
        &mut self,
        peer_stream: TcpStream,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) -> anyhow::Result<()> {
        let mut inbound_stream = InboundStream::new(peer_stream, self.replication.clone());
        inbound_stream.recv_handshake().await?;
        inbound_stream.disseminate_peers(self.members.keys().cloned().collect::<Vec<_>>()).await?;
        inbound_stream.try_sync_for_replica(logger).await?;
        let add_peer_cmd = inbound_stream.into_add_peer(self.self_handler.clone())?;
        self.add_peer(add_peer_cmd).await;
        Ok(())
    }

    pub(crate) fn set_replication_info(&mut self, leader_repl_id: ReplicationId, hwm: u64) {
        self.replication.replid = leader_repl_id;
        self.replication.hwm.store(hwm, Ordering::Release);
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

        self.remove_peers(to_be_removed).await;
    }

    async fn remove_peers(&mut self, to_be_removed: Vec<PeerIdentifier>) {
        for peer_id in to_be_removed {
            self.remove_peer(&peer_id).await;
        }
    }

    pub(crate) async fn gossip(
        &mut self,
        mut hop_count: u8,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        // If hop_count is 0, don't send the message to other peers
        if hop_count == 0 {
            return;
        };
        hop_count -= 1;
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

    pub(crate) async fn apply_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        self.merge_ban_list(ban_list);
        self.retain_only_recent_banned_nodes();

        // the following should be removed immediately
        let to_be_removed =
            self.replication.ban_list.iter().map(|node| node.p_id.clone()).collect::<Vec<_>>();

        self.remove_peers(to_be_removed).await;
    }

    pub(crate) fn update_on_hertbeat_message(&mut self, from: &PeerIdentifier, log_index: u64) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.last_seen = Instant::now();

            if let NodeKind::Replica = peer.kind() {
                peer.peer_state.match_index = log_index;
            }
        }
    }
    pub(crate) async fn try_create_append_entries(
        &mut self,
        logger: &mut ReplicatedLogs<impl TWriteAheadLog>,
        log: &WriteRequest,
    ) -> Result<Vec<WriteOperation>, ConsensusClientResponse> {
        if !self.replication.is_leader_mode {
            return Err(ConsensusClientResponse::Err("Write given to follower".into()));
        }

        let Ok(append_entries) = logger
            .leader_write_entries(log, self.take_low_watermark(), self.replication.term)
            .await
        else {
            return Err(ConsensusClientResponse::Err("Write operation failed".into()));
        };

        // Skip consensus for no replicas
        if self.replicas().count() == 0 {
            return Err(ConsensusClientResponse::LogIndex(Some(logger.last_log_index)));
        }

        Ok(append_entries)
    }

    pub(crate) async fn req_consensus(
        &mut self,
        logger: &mut ReplicatedLogs<impl TWriteAheadLog>,
        log: WriteRequest,
        callback: tokio::sync::oneshot::Sender<ConsensusClientResponse>,
        session_req: Option<SessionRequest>,
    ) {
        let (prev_log_index, prev_term) = (logger.last_log_index, logger.last_log_term);
        let append_entries = match self.try_create_append_entries(logger, &log).await {
            Ok(entries) => entries,
            Err(err) => {
                let _ = callback.send(err);
                return;
            },
        };

        self.consensus_tracker.add(
            logger.last_log_index,
            callback,
            self.replicas().count(),
            session_req,
        );

        // dbg!(self.replicas().count()); // CHECKED. replica count right.
        self.generate_follower_entries(append_entries, prev_log_index, prev_term)
            .map(|(peer, hb)| peer.send_to_peer(AppendEntriesRPC(hb)))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) async fn install_leader_state(
        &mut self,
        logs: Vec<WriteOperation>,
        cache_manager: &CacheManager,
    ) {
        println!("[INFO] Received Leader State - length {}", logs.len());
        if logs.is_empty() {
            return;
        }
        let last_log_idx = logs.last().unwrap().log_index;
        for log in logs {
            let _ = cache_manager.apply_log(log.request).await;
        }
        self.replication.hwm.store(last_log_idx, Ordering::Release);
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
            .filter_map(|peer| match peer.kind() {
                NodeKind::Replica => Some(peer.match_index()),
                _ => None,
            })
            .min()
    }

    pub(crate) fn track_replication_progress(
        &mut self,
        res: ReplicationResponse,
        sessions: &mut ClientSessions,
    ) {
        let Some(mut consensus) = self.consensus_tracker.remove(&res.log_idx) else {
            return;
        };

        if consensus.votable(&res.from) {
            println!("[INFO] Received acks for log index num: {}", res.log_idx);
            consensus.increase_vote(res.from);
        }
        if consensus.cnt < consensus.get_required_votes() {
            self.consensus_tracker.insert(res.log_idx, consensus);
            return;
        }
        sessions.set_response(consensus.session_req.take());
        let _ = consensus.callback.send(ConsensusClientResponse::LogIndex(Some(res.log_idx)));
    }

    // After send_ack: Leader updates its knowledge of follower's progress
    async fn send_ack(
        &mut self,
        send_to: &PeerIdentifier,
        log_idx: u64,
        rejection_reason: RejectionReason,
    ) {
        if let Some(leader) = self.members.get_mut(send_to) {
            let _ = leader
                .send_to_peer(ReplicationResponse::new(
                    log_idx,
                    rejection_reason,
                    &self.replication,
                ))
                .await;
        }
    }

    pub(crate) async fn send_commit_heartbeat(&mut self, offset: u64) {
        // TODO is there any case where I can use offset input?
        self.replication.hwm.fetch_add(1, Ordering::Relaxed);

        let message: HeartBeatMessage =
            self.replication.default_heartbeat(0, offset, self.replication.term);
        println!("[INFO] log {} commited", message.hwm);
        self.send_to_replicas(AppendEntriesRPC(message)).await;
    }

    pub(crate) async fn replicate(
        &mut self,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        mut heartbeat: HeartBeatMessage,
        cache_manager: &CacheManager,
    ) {
        // * logging case
        if self.try_replicate_logs(wal, &mut heartbeat).await.is_err() {
            return;
        };

        // * state machine case
        self.replicate_state(heartbeat.hwm, wal, cache_manager).await;
    }

    async fn try_replicate_logs(
        &mut self,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        rpc: &mut HeartBeatMessage,
    ) -> anyhow::Result<()> {
        if rpc.append_entries.is_empty() {
            return Ok(());
        }

        if let Err(e) =
            self.ensure_prev_consistency(wal, rpc.prev_log_index, rpc.prev_log_term).await
        {
            self.send_ack(&rpc.from, wal.last_log_index, e).await;
            return Err(anyhow::anyhow!("Fail fail to append"));
        }

        let match_index =
            wal.follower_write_entries(std::mem::take(&mut rpc.append_entries)).await?;

        self.send_ack(&rpc.from, match_index, RejectionReason::None).await;
        Ok(())
    }

    async fn ensure_prev_consistency(
        &self,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        prev_log_index: u64,
        prev_log_term: u64,
    ) -> Result<(), RejectionReason> {
        // Case 1: Empty log
        if wal.is_empty() {
            if prev_log_index == 0 {
                return Ok(()); // First entry, no previous log to check
            }
            println!("[ERROR] Log is empty but leader expects an entry");
            return Err(RejectionReason::LogInconsistency); // Log empty but leader expects an entry
        }

        // Case 2: Previous index is before the log's start (compacted/truncated)
        if prev_log_index < wal.log_start_index() {
            println!("[ERROR] Previous log index is before the log's start");
            return Err(RejectionReason::LogInconsistency); // Leader references an old, unavailable entry
        }

        // * Raft followers should truncate their log starting at prev_log_index + 1 and then append the new entries
        // * Just returning an error is breaking consistency
        if let Some(prev_entry) = wal.read_at(prev_log_index).await {
            println!("[INFO] Previous log entry: {:?}", prev_entry);
            if prev_entry.term != prev_log_term {
                // ! Term mismatch -> triggers log truncation
                println!("[ERROR] Term mismatch: {} != {}", prev_entry.term, prev_log_term);
                wal.truncate_after(prev_log_index).await;

                return Err(RejectionReason::LogInconsistency);
            }
        }

        Ok(())
    }

    pub(crate) async fn send_leader_heartbeat(
        &mut self,
        logger: &ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        self.send_to_replicas(AppendEntriesRPC(self.replication.default_heartbeat(
            0,
            logger.last_log_index,
            logger.last_log_term,
        )))
        .await;
    }

    async fn send_to_replicas(&mut self, msg: impl Into<QueryIO> + Send + Clone) {
        self.replicas_mut()
            .map(|(peer, _)| peer.send_to_peer(msg.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) fn cluster_nodes(&self) -> Vec<ClusterNode> {
        self.members
            .values()
            .map(ClusterNode::from_peer)
            .chain(std::iter::once(self.replication.self_info()))
            .collect()
    }

    pub(crate) async fn run_for_election(
        &mut self,
        logger: &mut ReplicatedLogs<impl TWriteAheadLog>,
    ) {
        let ElectionState::Follower { voted_for: None } = &self.replication.election_state else {
            return;
        };

        self.become_candidate();
        let request_vote =
            RequestVote::new(&self.replication, logger.last_log_index, logger.last_log_index);

        println!("[INFO] Running for election term {}", self.replication.term);
        self.replicas_mut()
            .map(|(peer, _)| peer.send_to_peer(request_vote.clone()))
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
        let _ = peer.send_to_peer(RequestVoteReply { term, vote_granted: grant_vote }).await;
    }

    pub(crate) async fn tally_vote(&mut self, logger: &ReplicatedLogs<impl TWriteAheadLog>) {
        if !self.replication.election_state.may_become_leader() {
            return;
        }
        self.become_leader().await;

        let msg =
            self.replication.default_heartbeat(0, logger.last_log_index, logger.last_log_term);
        self.replicas_mut()
            .map(|(peer, _)| peer.send_to_peer(AppendEntriesRPC(msg.clone())))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) fn reset_election_timeout(&mut self, leader_id: &PeerIdentifier) {
        if let Some(peer) = self.members.get_mut(leader_id) {
            peer.last_seen = Instant::now();
        }
        self.heartbeat_scheduler.reset_election_timeout();
        self.replication.election_state = ElectionState::Follower { voted_for: None };
    }

    async fn replicate_state(
        &mut self,
        heartbeat_hwm: u64,
        wal: &mut ReplicatedLogs<impl TWriteAheadLog>,
        cache_manager: &CacheManager,
    ) {
        let old_hwm = self.replication.hwm.load(Ordering::Acquire);
        if heartbeat_hwm > old_hwm {
            println!("[INFO] Received commit offset {}", heartbeat_hwm);

            for log_index in (old_hwm + 1)..=heartbeat_hwm {
                let Some(log) = wal.read_at(log_index).await else {
                    println!("[ERROR] log has never been replicated!");
                    return;
                };

                if let Err(e) = cache_manager.apply_log(log.request).await {
                    println!("[ERROR] Failed to apply log: {:?}", e);
                    return; // Stop on first error
                }
            }
            self.replication.hwm.store(heartbeat_hwm, Ordering::Release);
        }
    }

    pub(crate) fn maybe_update_term(&mut self, new_term: u64) {
        if new_term > self.replication.term {
            self.replication.term = new_term;
            self.replication.election_state = ElectionState::Follower { voted_for: None };
            self.replication.is_leader_mode = false;
            self.replication.role = ReplicationRole::Follower;
        }
    }

    pub(crate) async fn check_term_outdated(
        &mut self,
        heartbeat: &HeartBeatMessage,
        wal: &ReplicatedLogs<impl TWriteAheadLog>,
    ) -> bool {
        if heartbeat.term < self.replication.term {
            self.send_ack(
                &heartbeat.from,
                wal.last_log_index,
                RejectionReason::ReceiverHasHigherTerm,
            )
            .await;
            return true;
        }
        false
    }

    /// Used when:
    /// 1) on follower's consensus rejection when term is not matched
    /// 2) step down operation is given from user
    pub(crate) async fn step_down(&mut self) {
        self.replication.vote_for(None);
        self.heartbeat_scheduler.turn_follower_mode().await;
    }

    async fn become_leader(&mut self) {
        eprintln!("\x1b[32m[INFO] Election succeeded\x1b[0m");
        self.replication.become_leader();
        self.heartbeat_scheduler.turn_leader_mode().await;
    }
    fn become_candidate(&mut self) {
        self.replication.become_candidate(self.replicas().count() as u8);
    }

    pub(crate) async fn handle_repl_rejection(&mut self, repl_res: ReplicationResponse) {
        match repl_res.rej_reason {
            RejectionReason::ReceiverHasHigherTerm => self.step_down().await,
            RejectionReason::LogInconsistency => {
                self.decrease_match_index(&repl_res.from);
            },
            RejectionReason::None => (),
        }
    }

    fn decrease_match_index(&mut self, from: &PeerIdentifier) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.peer_state.match_index -= 1;
        }
    }

    pub(crate) async fn replicaof(&mut self, peer_addr: PeerIdentifier) {
        self.replication.vote_for(Some(peer_addr));
        self.set_replication_info(ReplicationId::Undecided, 0);
        self.heartbeat_scheduler.turn_follower_mode().await;
    }

    pub(crate) async fn join_peer_network_if_absent(&mut self, cluster_nodes: Vec<ClusterNode>) {
        let peers = cluster_nodes
            .into_iter()
            .filter(|n| n.bind_addr != self.replication.self_identifier())
            .filter(|p| !self.members.contains_key(&p.bind_addr))
            .map(|node| node.bind_addr)
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return;
        }

        for peer in peers {
            if self.replication.in_ban_list(&peer) {
                continue;
            }

            if self.discover_cluster(peer).await.is_ok() {
                let _ = self.snapshot_topology().await;
                break;
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_variables)]
mod test {
    use super::*;
    use crate::adapters::op_logs::memory_based::MemoryOpLogs;
    use crate::domains::caches::actor::CacheCommandSender;
    use crate::domains::caches::cache_objects::CacheEntry;
    use crate::domains::caches::command::CacheCommand;
    use crate::domains::cluster_actors::commands::ClusterCommand;
    use crate::domains::cluster_actors::listener::PeerListener;
    use crate::domains::cluster_actors::replication::ReplicationRole;
    use crate::domains::operation_logs::WriteOperation;
    use crate::domains::operation_logs::WriteRequest;
    use crate::domains::peers::peer::PeerState;

    use std::ops::Range;
    use std::time::Duration;
    use tokio::fs::OpenOptions;

    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::channel;
    use uuid::Uuid;

    fn write_operation_create_helper(
        index_num: u64,
        term: u64,
        key: &str,
        value: &str,
    ) -> WriteOperation {
        WriteOperation {
            log_index: index_num.into(),
            request: WriteRequest::Set { key: key.into(), value: value.into() },
            term,
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
            prev_log_index: if !op_logs.is_empty() { op_logs[0].log_index - 1 } else { 0 },
            prev_log_term: 0,
            append_entries: op_logs,
            ban_list: vec![],
            from: PeerIdentifier::new("localhost", 8080),
            replid: ReplicationId::Key("localhost".to_string().into()),
            hop_count: 0,
            cluster_nodes: vec![],
        }
    }

    async fn cluster_actor_create_helper() -> ClusterActor {
        let replication = ReplicationState::new(
            ReplicationId::Key("master".into()),
            ReplicationRole::Leader,
            "localhost",
            8080,
        );

        let topology_writer = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("duva.tp")
            .await
            .unwrap();

        ClusterActor::new(100, replication, 100, topology_writer)
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
            let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();
            let kill_switch = PeerListener::spawn(r, cluster_sender.clone(), key.clone());
            actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new(
                    x,
                    PeerState::new(
                        PeerIdentifier::new("localhost", port),
                        follower_hwm,
                        ReplicationId::Key("localhost".to_string().into()),
                        NodeKind::Replica,
                    ),
                    kill_switch,
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
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;
        let (tx, rx) = tokio::sync::oneshot::channel();

        // WHEN
        cluster_actor
            .req_consensus(
                &mut logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                tx,
                None,
            )
            .await;

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(logger.last_log_index, 1);
    }

    #[tokio::test]
    async fn req_consensus_inserts_consensus_voting() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);

        let mut cluster_actor = cluster_actor_create_helper().await;

        // - add 5 followers
        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };

        cluster_member_create_helper(&mut cluster_actor, 0..5, cluster_sender, cache_manager, 0)
            .await;

        let (tx, _) = tokio::sync::oneshot::channel();
        let client_id = Uuid::now_v7();
        let session_request = SessionRequest::new(1, client_id);
        // WHEN
        cluster_actor
            .req_consensus(
                &mut logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                tx,
                Some(session_request.clone()),
            )
            .await;

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(logger.last_log_index, 1);

        assert_eq!(
            cluster_actor.consensus_tracker.get(&1).unwrap().session_req.as_ref().unwrap().clone(), //* session_request_is_saved_on_tracker
            session_request
        );
    }

    #[tokio::test]
    async fn test_leader_req_consensus_early_return_when_already_processed_session_req_given() {
        // GIVEN
        let logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let cluster_actor = cluster_actor_create_helper().await;

        let cache_manager = CacheManager { inboxes: vec![] };

        let mut sessions = ClientSessions::default();
        let client_id = Uuid::now_v7();
        let client_req = SessionRequest::new(1, client_id);

        // WHEN - session request is already processed
        sessions.set_response(Some(client_req.clone()));
        let handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(MemoryOpLogs::default(), cache_manager, sessions));
        let (tx, rx) = tokio::sync::oneshot::channel();
        handler
            .send(ClusterCommand::LeaderReqConsensus {
                log: WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                callback: tx,
                session_req: Some(client_req),
            })
            .await
            .unwrap();

        // THEN
        rx.await.unwrap();
    }

    #[tokio::test]
    async fn test_consensus_voting_deleted_when_consensus_reached() {
        // GIVEN
        let mut sessions = ClientSessions::default();
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        // - add followers to create quorum
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(&mut cluster_actor, 0..4, cluster_sender, cache_manager, 0)
            .await;
        let (client_request_sender, client_wait) = tokio::sync::oneshot::channel();

        let client_id = Uuid::now_v7();
        let client_request = SessionRequest::new(3, client_id);
        cluster_actor
            .req_consensus(
                &mut logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                client_request_sender,
                Some(client_request.clone()),
            )
            .await;

        // WHEN
        let follower_res = ReplicationResponse {
            log_idx: 1,
            term: 0,
            rej_reason: RejectionReason::None,
            from: PeerIdentifier("".into()),
        };
        cluster_actor
            .track_replication_progress(follower_res.clone().set_from("repl1"), &mut sessions);
        cluster_actor
            .track_replication_progress(follower_res.clone().set_from("repl2"), &mut sessions);

        // up to this point, tracker hold the consensus
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(cluster_actor.consensus_tracker.get(&1).unwrap().voters.len(), 2);

        // ! Majority votes made
        cluster_actor.track_replication_progress(follower_res.set_from("repl3"), &mut sessions);

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(logger.last_log_index, 1);

        client_wait.await.unwrap();
        assert!(sessions.is_processed(&Some(client_request))); // * session_request_is_marked_as_processed
    }

    #[tokio::test]
    async fn test_same_voter_can_vote_only_once() {
        // GIVEN
        let mut sessions = ClientSessions::default();
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        // - add followers to create quorum
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(&mut cluster_actor, 0..4, cluster_sender, cache_manager, 0)
            .await;
        let (client_request_sender, client_wait) = tokio::sync::oneshot::channel();

        cluster_actor
            .req_consensus(
                &mut logger,
                WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                client_request_sender,
                None,
            )
            .await;

        // WHEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        let follower_res = ReplicationResponse {
            log_idx: 1,
            term: 0,
            rej_reason: RejectionReason::None,
            from: PeerIdentifier("repl1".into()), //TODO Must be changed if "update_match_index" becomes idempotent operation on peer id
        };
        cluster_actor.track_replication_progress(follower_res.clone(), &mut sessions);
        cluster_actor.track_replication_progress(follower_res.clone(), &mut sessions);
        cluster_actor.track_replication_progress(follower_res.clone(), &mut sessions);

        // THEN - no change in consensus tracker even though the same voter voted multiple times
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(logger.last_log_index, 1);
    }

    #[tokio::test]
    async fn logger_create_entries_from_lowest() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);

        let test_logs = vec![
            write_operation_create_helper(1, 0, "foo", "bar"),
            write_operation_create_helper(2, 0, "foo2", "bar"),
            write_operation_create_helper(3, 0, "foo3", "bar"),
        ];
        logger.follower_write_entries(test_logs.clone()).await.unwrap();

        // WHEN
        const LOWEST_FOLLOWER_COMMIT_INDEX: u64 = 2;
        let logs = logger
            .leader_write_entries(
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
        assert_eq!(logger.last_log_index, 4);
    }

    #[tokio::test]
    async fn generate_follower_entries() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);

        let mut cluster_actor = cluster_actor_create_helper().await;

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
            write_operation_create_helper(1, 0, "foo", "bar"),
            write_operation_create_helper(2, 0, "foo2", "bar"),
            write_operation_create_helper(3, 0, "foo3", "bar"),
        ];

        cluster_actor.replication.hwm.store(3, Ordering::Release);

        logger.follower_write_entries(test_logs).await.unwrap();

        //WHEN
        // *add lagged followers with its commit index being 1
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(&mut cluster_actor, 5..7, cluster_sender, cache_manager, 1)
            .await;

        // * add new log - this must create entries that are greater than 3
        let lowest_hwm = cluster_actor.take_low_watermark();
        let append_entries = logger
            .leader_write_entries(
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
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;
        // WHEN - term
        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, 0, "foo", "bar"),
                write_operation_create_helper(2, 0, "foo2", "bar"),
            ],
        );
        let cache_manager = CacheManager {
            inboxes: (0..10).map(|_| CacheCommandSender(channel(10).0)).collect::<Vec<_>>(),
        };
        cluster_actor.replicate(&mut logger, heartbeat, &cache_manager).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 0);
        assert_eq!(logger.last_log_index, 2);
        let logs = logger.range(0, 2).await;
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 1);
        assert_eq!(logs[1].log_index, 2);
        assert_eq!(logs[0].request, WriteRequest::Set { key: "foo".into(), value: "bar".into() });
        assert_eq!(logs[1].request, WriteRequest::Set { key: "foo2".into(), value: "bar".into() });
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let (cache_handler, mut receiver) = tokio::sync::mpsc::channel(100);
        let mut cluster_actor = cluster_actor_create_helper().await;

        let heartbeat = heartbeat_create_helper(
            0,
            0,
            vec![
                write_operation_create_helper(1, 0, "foo", "bar"),
                write_operation_create_helper(2, 0, "foo2", "bar"),
            ],
        );

        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(cache_handler)] };
        cluster_actor.replicate(&mut logger, heartbeat, &cache_manager).await;

        // WHEN - commit until 2
        let task = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    CacheCommand::Set { cache_entry: CacheEntry::KeyValue { key, value } } => {
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
        cluster_actor.replicate(&mut logger, heartbeat, &cache_manager).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 2);
        assert_eq!(logger.last_log_index, 2);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_multiple_committed_entries() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;

        // Add multiple entries
        let entries = vec![
            write_operation_create_helper(1, 0, "key1", "value1"),
            write_operation_create_helper(2, 0, "key2", "value2"),
            write_operation_create_helper(3, 0, "key3", "value3"),
        ];

        let heartbeat = heartbeat_create_helper(1, 0, entries);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        // First append entries but don't commit
        cluster_actor.replicate(&mut logger, heartbeat, &cache_manager).await;

        // Create a task to monitor applied entries
        let monitor_task = tokio::spawn(async move {
            let mut applied_keys = Vec::new();

            while let Some(message) = rx.recv().await {
                if let CacheCommand::Set { cache_entry: CacheEntry::KeyValue { key, .. } } = message
                {
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

        cluster_actor.replicate(&mut logger, commit_heartbeat, &cache_manager).await;

        // THEN
        // Verify that all entries were committed and applied in order
        let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
            .await
            .expect("Timeout waiting for entries to be applied")
            .expect("Task failed");

        assert_eq!(applied_keys, vec!["key1", "key2", "key3"]);
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_partial_commit_with_new_entries() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;

        // First, append some entries
        let first_entries = vec![
            write_operation_create_helper(1, 0, "key1", "value1"),
            write_operation_create_helper(2, 0, "key2", "value2"),
        ];

        let first_heartbeat = heartbeat_create_helper(1, 0, first_entries);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        cluster_actor.replicate(&mut logger, first_heartbeat, &cache_manager).await;

        // Create a task to monitor applied entries
        let monitor_task = tokio::spawn(async move {
            let mut applied_keys = Vec::new();

            while let Some(message) = rx.recv().await {
                if let CacheCommand::Set { cache_entry: CacheEntry::KeyValue { key, .. } } = message
                {
                    applied_keys.push(key);
                    if applied_keys.len() == 1 {
                        break;
                    }
                }
            }

            applied_keys
        });

        // WHEN - commit partial entries and append new ones
        let second_entries = vec![write_operation_create_helper(3, 0, "key3", "value3")];

        let second_heartbeat = heartbeat_create_helper(1, 1, second_entries);

        cluster_actor.replicate(&mut logger, second_heartbeat, &cache_manager).await;

        // THEN
        // Verify that only key1 was applied
        let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
            .await
            .expect("Timeout waiting for entries to be applied")
            .expect("Task failed");

        assert_eq!(applied_keys, vec!["key1"]);
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 1);
        assert_eq!(logger.last_log_index, 3); // All entries are in the log
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state_only_upto_hwm() {
        // GIVEN
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);

        let mut cluster_actor = cluster_actor_create_helper().await;

        // Log two entries but don't commit them yet
        let heartbeat = heartbeat_create_helper(
            0,
            0, // hwm=0, nothing committed yet
            vec![
                write_operation_create_helper(1, 0, "foo", "bar"),
                write_operation_create_helper(2, 0, "foo2", "bar"),
            ],
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        // This just appends the entries to the log but doesn't commit them
        cluster_actor.replicate(&mut logger, heartbeat, &cache_manager).await;

        // WHEN - commit only up to index 1
        let task = tokio::spawn(async move {
            let mut received_foo = false;

            while let Some(message) = rx.recv().await {
                match message {
                    CacheCommand::Set { cache_entry: CacheEntry::KeyValue { key, value } } => {
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
        cluster_actor.replicate(&mut logger, heartbeat, &cache_manager).await;

        // THEN
        // Give the task a chance to process the message
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the task since we're done checking
        task.abort();

        // Verify state
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 1);
        assert_eq!(logger.last_log_index, 2);
    }

    #[tokio::test]
    async fn follower_truncates_log_on_term_mismatch() {
        // GIVEN: A follower with an existing log entry at index 1, term 1
        let mut inmemory = MemoryOpLogs::default();
        //prefill

        inmemory.writer.extend(vec![
            write_operation_create_helper(2, 1, "key1", "val1"),
            write_operation_create_helper(3, 1, "key2", "val2"),
        ]);

        let mut logger = ReplicatedLogs::new(inmemory, 3, 1);
        let mut cluster_actor = cluster_actor_create_helper().await;

        assert_eq!(logger.target.writer.len(), 2);

        // Simulate an initial log entry at index 1, term 1
        // WHEN: Leader sends an AppendEntries with prev_log_index=1, prev_log_term=2 (mismatch)
        let mut heartbeat = heartbeat_create_helper(
            2,
            0,
            vec![write_operation_create_helper(2, 0, "key2", "val2")],
        );
        heartbeat.prev_log_term = 0;
        heartbeat.prev_log_index = 2;

        let result = cluster_actor.try_replicate_logs(&mut logger, &mut heartbeat).await;

        // THEN: Expect truncation and rejection
        assert_eq!(logger.target.writer.len(), 1);
        assert!(result.is_err(), "Should reject due to term mismatch");
    }

    #[tokio::test]
    async fn follower_accepts_entries_with_empty_log_and_prev_log_index_zero() {
        // GIVEN: A follower with an empty log
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;

        // WHEN: Leader sends entries with prev_log_index=0
        let mut heartbeat = heartbeat_create_helper(
            1,
            0,
            vec![write_operation_create_helper(1, 0, "key1", "val1")],
        );

        let result = cluster_actor.try_replicate_logs(&mut logger, &mut heartbeat).await;

        // THEN: Entries are accepted
        assert!(result.is_ok(), "Should accept entries with prev_log_index=0 on empty log");
        assert_eq!(logger.last_log_index, 1); // Assuming write_log_entries updates log_index
    }

    #[tokio::test]
    async fn follower_rejects_entries_with_empty_log_and_prev_log_index_nonzero() {
        // GIVEN: A follower with an empty log
        let mut logger = ReplicatedLogs::new(MemoryOpLogs::default(), 0, 0);
        let mut cluster_actor = cluster_actor_create_helper().await;

        // WHEN: Leader sends entries with prev_log_index=1
        let mut heartbeat = heartbeat_create_helper(
            1,
            0,
            vec![write_operation_create_helper(2, 0, "key2", "val2")],
        );
        heartbeat.prev_log_index = 1;
        heartbeat.prev_log_term = 1;

        let result = cluster_actor.try_replicate_logs(&mut logger, &mut heartbeat).await;

        // THEN: Entries are rejected
        assert!(result.is_err(), "Should reject entries with prev_log_index > 0 on empty log");
        assert_eq!(logger.last_log_index, 0); // Log should remain unchanged
    }

    #[tokio::test]
    async fn test_cluster_nodes() {
        use std::io::Write;
        use tokio::net::TcpListener;
        // GIVEN

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();

        let mut cluster_actor = cluster_actor_create_helper().await;

        let repl_id = cluster_actor.replication.replid.clone();

        // followers
        for port in [6379, 6380] {
            let key = PeerIdentifier::new("127.0.0.1", port);
            let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();
            let kill_switch = PeerListener::spawn(r, cluster_sender.clone(), key.clone());
            cluster_actor.members.insert(
                key.clone(),
                Peer::new(
                    x,
                    PeerState::new(key, 0, repl_id.clone(), NodeKind::Replica),
                    kill_switch,
                ),
            );
        }

        let listener_for_second_shard = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr_for_second_shard = listener_for_second_shard.local_addr().unwrap();
        let second_shard_leader_identifier: PeerIdentifier =
            listener_for_second_shard.local_addr().unwrap().to_string().into();
        let second_shard_repl_id = uuid::Uuid::now_v7();

        // leader for different shard?
        let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();
        let kill_switch =
            PeerListener::spawn(r, cluster_sender.clone(), second_shard_leader_identifier.clone());

        cluster_actor.members.insert(
            second_shard_leader_identifier.clone(),
            Peer::new(
                x,
                PeerState::new(
                    second_shard_leader_identifier.clone(),
                    0,
                    ReplicationId::Key(second_shard_repl_id.to_string()),
                    NodeKind::NonData,
                ),
                kill_switch,
            ),
        );

        // follower for different shard
        for port in [2655, 2653] {
            let key = PeerIdentifier::new("127.0.0.1", port);
            let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();
            let kill_switch = PeerListener::spawn(r, cluster_sender.clone(), key.clone());

            cluster_actor.members.insert(
                key.clone(),
                Peer::new(
                    x,
                    PeerState::new(
                        key,
                        0,
                        ReplicationId::Key(second_shard_repl_id.to_string()),
                        NodeKind::NonData,
                    ),
                    kill_switch,
                ),
            );
        }

        // WHEN
        let res = cluster_actor.cluster_nodes();
        let repl_id = cluster_actor.replication.replid.clone();
        assert_eq!(res.len(), 6);

        let file_content = format!(
            r#"
        127.0.0.1:6379 {repl_id} 0
        127.0.0.1:6380 {repl_id} 0
        {second_shard_leader_identifier} {second_shard_repl_id} 0
        127.0.0.1:2655 {second_shard_repl_id} 0
        127.0.0.1:2653 {second_shard_repl_id} 0
        localhost:8080 myself,{repl_id} 0
        "#
        );
        let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        write!(temp_file, "{}", file_content).expect("Failed to write to temp file");
        let nodes = ClusterNode::from_file(temp_file.path().to_str().unwrap());

        for value in nodes {
            assert!(res.contains(&value));
        }
    }

    #[tokio::test]
    async fn test_store_current_topology() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;
        let path = "test_store_current_topology.tp";
        cluster_actor.topology_writer = tokio::fs::File::create(path).await.unwrap();

        let repl_id = cluster_actor.replication.replid.clone();
        let self_id = cluster_actor.replication.self_identifier();

        // WHEN
        cluster_actor.snapshot_topology().await.unwrap();

        // THEN
        let topology = tokio::fs::read_to_string(path).await.unwrap();
        let expected_topology = format!("{} myself,{} 0", self_id, repl_id);
        assert_eq!(topology, expected_topology);

        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_snapshot_topology_after_add_peer() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;
        let path = "test_snapshot_topology_after_add_peer.tp";
        cluster_actor.topology_writer = tokio::fs::File::create(path).await.unwrap();

        let repl_id = cluster_actor.replication.replid.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();
        let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();

        let kill_switch = PeerListener::spawn(
            r,
            cluster_actor.self_handler.clone(),
            PeerIdentifier("127.0.0.1:3849".into()),
        );

        let peer = Peer::new(
            x,
            PeerState::new(
                "127.0.0.1:3849".to_string().into(),
                0,
                ReplicationId::Key(repl_id.to_string()),
                NodeKind::Replica,
            ),
            kill_switch,
        );

        // WHEN
        let add_peer_cmd =
            AddPeer { peer_id: PeerIdentifier(String::from("127.0.0.1:3849")), peer };
        cluster_actor.add_peer(add_peer_cmd).await;
        cluster_actor.snapshot_topology().await.unwrap();

        // THEN
        let topology = tokio::fs::read_to_string(path).await.unwrap();
        let mut cluster_nodes =
            topology.split("\r\n").map(|x| x.to_string()).collect::<Vec<String>>();

        cluster_nodes.dedup();
        assert_eq!(cluster_nodes.len(), 2);

        for value in [
            format!("127.0.0.1:3849 {} 0", repl_id),
            format!("{} myself,{} 0", cluster_actor.replication.self_identifier(), repl_id),
        ] {
            assert!(cluster_nodes.contains(&value));
        }

        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_reconnection_on_gossip() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;

        // * run listener to see if connection is attempted
        let listener = TcpListener::bind("127.0.0.1:44455").await.unwrap(); // ! Beaware that this is cluster port
        let bind_addr = listener.local_addr().unwrap();

        let mut replication_state = cluster_actor.replication.clone();
        replication_state.is_leader_mode = false;

        let (tx, rx) = tokio::sync::oneshot::channel();

        // Spawn the listener task
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut inbound_stream = InboundStream::new(stream, replication_state.clone());
            if inbound_stream.recv_handshake().await.is_ok() {
                let _ = tx.send(());
            };
        });

        // WHEN - try to reconnect
        cluster_actor
            .join_peer_network_if_absent(vec![ClusterNode {
                bind_addr: PeerIdentifier::new("127.0.0.1", bind_addr.port() - 10000),
                repl_id: cluster_actor.replication.replid.clone().to_string(),
                is_myself: false,
                kind: NodeKind::Replica,
            }])
            .await;

        assert!(handle.await.is_ok());
        assert!(rx.await.is_ok());
    }
}
