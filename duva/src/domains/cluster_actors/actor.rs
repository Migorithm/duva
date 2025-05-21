use super::ClusterCommand;
use super::ConsensusClientResponse;
use super::ConsensusRequest;
use super::LazyOption;
use super::heartbeat_scheduler::HeartBeatScheduler;
use super::replication::ReplicationId;
use super::replication::ReplicationRole;
use super::replication::ReplicationState;
use super::replication::time_in_secs;
use super::session::ClientSessions;
use super::*;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::consensus::ElectionState;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::ElectionVote;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::RejectionReason;
use crate::domains::peers::command::ReplicationAck;
use crate::domains::peers::command::RequestVote;
use crate::domains::peers::connections::inbound::stream::InboundStream;
use crate::domains::peers::connections::outbound::stream::OutboundStream;
use crate::domains::peers::peer::NodeKind;
use crate::domains::peers::peer::PeerState;
use crate::domains::query_parsers::QueryIO;
use crate::err;
use std::collections::VecDeque;
use std::iter;
use std::sync::atomic::Ordering;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

#[derive(Debug)]
pub struct ClusterActor<T> {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationState,
    pub(crate) node_timeout: u128,
    pub(crate) consensus_tracker: LogConsensusTracker,
    pub(crate) receiver: tokio::sync::mpsc::Receiver<ClusterCommand>,
    pub(crate) self_handler: ClusterCommandHandler,
    pub(crate) heartbeat_scheduler: HeartBeatScheduler,
    pub(crate) topology_writer: tokio::fs::File,
    pub(crate) node_change_broadcast: tokio::sync::broadcast::Sender<Vec<PeerIdentifier>>,

    // * Pending requests are used to store requests that are received while the actor is in the process of election/cluster rebalancing.
    // * These requests will be processed once the actor is back to a stable state.
    pub(crate) pending_requests: Option<VecDeque<ConsensusRequest>>,
    pub(crate) client_sessions: ClientSessions,
    pub(crate) logger: ReplicatedLogs<T>,
}

#[derive(Debug, Clone)]
pub struct ClusterCommandHandler(tokio::sync::mpsc::Sender<ClusterCommand>);
impl ClusterCommandHandler {
    pub(crate) async fn send(
        &self,
        cmd: impl Into<ClusterCommand>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ClusterCommand>> {
        self.0.send(cmd.into()).await
    }
}

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(crate) fn new(
        node_timeout: u128,
        init_repl_info: ReplicationState,
        heartbeat_interval_in_mills: u64,
        topology_writer: File,
        log_writer: T,
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
            self_handler: ClusterCommandHandler(self_handler),
            members: BTreeMap::new(),
            consensus_tracker: LogConsensusTracker::default(),
            topology_writer,
            node_change_broadcast: tx,
            pending_requests: None,
            client_sessions: ClientSessions::default(),

            // todo initial value setting
            logger: ReplicatedLogs::new(log_writer, 0, 0),
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

    pub(crate) async fn send_cluster_heartbeat(&mut self, hop_count: u8) {
        // TODO randomly choose the peer to send the message
        let msg = QueryIO::ClusterHeartBeat(
            self.replication
                .default_heartbeat(hop_count, self.logger.last_log_index, self.logger.last_log_term)
                .set_cluster_nodes(self.cluster_nodes()),
        );

        for peer in self.members.values_mut() {
            let _ = peer.send(msg.clone()).await;
        }
    }

    pub(crate) async fn snapshot_topology(&mut self) -> anyhow::Result<()> {
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

    pub(crate) async fn add_peer(&mut self, peer: Peer) {
        self.replication.ban_list.remove(peer.id());

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer.id().clone(), peer) {
            existing_peer.kill().await;
        }
        self.broadcast_topology_change();
        let _ = self.snapshot_topology().await;
    }

    // * Broadcasts the current topology to all connected clients
    fn broadcast_topology_change(&mut self) {
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
    async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        if let Some(peer) = self.members.remove(peer_addr) {
            warn!("{} is being removed!", peer_addr);
            // stop the runnin process and take the connection in case topology changes are made
            let _read_connected = peer.kill().await;
            return Some(());
        }
        None
    }

    pub(crate) async fn connect_to_server(
        &mut self,
        connect_to: PeerIdentifier,
        optional_callback: Option<tokio::sync::oneshot::Sender<anyhow::Result<()>>>,
    ) {
        let stream = match OutboundStream::new(connect_to, self.replication.clone()).await {
            | Ok(stream) => stream,
            | Err(e) => {
                if let Some(cb) = optional_callback {
                    let _ = cb.send(err!(e));
                }
                return;
            },
        };

        tokio::spawn(stream.add_peer(
            self.replication.self_port,
            self.self_handler.clone(),
            optional_callback,
        ));
    }

    pub(crate) async fn accept_inbound_stream(&mut self, peer_stream: TcpStream) {
        let inbound_stream = InboundStream::new(peer_stream, self.replication.clone());
        tokio::spawn(
            inbound_stream.add_peer(
                self.members.keys().cloned().collect::<Vec<_>>(),
                self.self_handler.clone(),
            ),
        );
    }

    pub(crate) fn set_repl_id(&mut self, leader_repl_id: ReplicationId) {
        self.replication.replid = leader_repl_id;
    }

    /// Remove the peers that are idle for more than ttl_mills
    pub(crate) async fn remove_idle_peers(&mut self) {
        // loop over members, if ttl is expired, remove the member
        let now = Instant::now();

        for peer_id in self
            .members
            .iter()
            .filter(|&(_, peer)| now.duration_since(peer.last_seen).as_millis() > self.node_timeout)
            .map(|(id, _)| id)
            .cloned()
            .collect::<Vec<_>>()
        {
            self.remove_peer(&peer_id).await;
        }
    }

    async fn gossip(&mut self, mut hop_count: u8) {
        // If hop_count is 0, don't send the message to other peers
        if hop_count == 0 {
            return;
        };
        hop_count -= 1;
        self.send_cluster_heartbeat(hop_count).await;
    }

    pub(crate) async fn forget_peer(
        &mut self,
        peer_addr: PeerIdentifier,
    ) -> anyhow::Result<Option<()>> {
        let res = self.remove_peer(&peer_addr).await;
        self.replication.ban_list.insert(BannedPeer { p_id: peer_addr, ban_time: time_in_secs()? });

        Ok(res)
    }

    fn merge_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        if ban_list.is_empty() {
            return;
        }

        // Retain the latest
        for banned_peer in ban_list {
            let ban_list = &mut self.replication.ban_list;

            if let Some(existing) = ban_list.take(&banned_peer) {
                let newer =
                    if banned_peer.ban_time > existing.ban_time { banned_peer } else { existing };
                ban_list.insert(newer);
            } else {
                ban_list.insert(banned_peer);
            }
        }
    }

    pub(crate) async fn handle_cluster_heartbeat(&mut self, mut heartbeat: HeartBeat) {
        if self.replication.in_ban_list(&heartbeat.from) {
            debug!("{} in the ban list", heartbeat.from);
            return;
        }
        self.apply_ban_list(std::mem::take(&mut heartbeat.ban_list)).await;
        self.join_peer_network_if_absent(heartbeat.cluster_nodes).await;
        self.gossip(heartbeat.hop_count).await;
        self.update_on_hertbeat_message(&heartbeat.from, heartbeat.hwm);
    }

    async fn apply_ban_list(&mut self, ban_list: Vec<BannedPeer>) {
        self.merge_ban_list(ban_list);
        // retain_only_recent_banned_nodes
        let current_time_in_sec = time_in_secs().unwrap();
        self.replication.ban_list.retain(|node| current_time_in_sec - node.ban_time < 60);

        for banned_peer in self.replication.ban_list.iter().cloned().collect::<Vec<_>>() {
            self.remove_peer(&banned_peer.p_id).await;
        }
    }

    fn update_on_hertbeat_message(&mut self, from: &PeerIdentifier, log_index: u64) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.last_seen = Instant::now();
            peer.set_match_index(log_index);
        }
    }

    pub(crate) async fn req_consensus(&mut self, req: ConsensusRequest) {
        if !self.replication.is_leader_mode {
            let _ = req.callback.send(err!("Write given to follower"));
            return;
        }
        if self.client_sessions.is_processed(&req.session_req) {
            // TODO mapping between early returned values to client result
            let _ = req.callback.send(Ok(ConsensusClientResponse::AlreadyProcessed {
                key: req.request.key(),
                index: self.logger.last_log_index,
            }));
            return;
        };

        if let Some(pending_requests) = self.pending_requests.as_mut() {
            pending_requests.push_back(req);
            return;
        }

        // * Check if the request has already been processed
        if let Err(err) = self.logger.write_single_entry(&req.request, self.replication.term).await
        {
            let _ = req.callback.send(Err(anyhow::anyhow!(err)));
            return;
        };

        if self.replicas().count() == 0 {
            // * If there are no replicas, we can send the response immediately
            self.replication.hwm.fetch_add(1, Ordering::Relaxed);
            req.callback
                .send(Ok(ConsensusClientResponse::LogIndex(self.logger.last_log_index.into())))
                .ok();
            return;
        }

        self.consensus_tracker.add(
            self.logger.last_log_index,
            req.callback,
            self.replicas().count(),
            req.session_req,
        );

        self.send_rpc_to_replicas().await;
    }

    async fn send_rpc_to_replicas(&mut self) {
        self.iter_follower_append_entries()
            .await
            .map(|(peer, hb)| peer.send(QueryIO::AppendEntriesRPC(hb)))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    /// Creates individualized append entries messages for each follower.
    ///
    /// This function generates customized heartbeat messages containing only the log entries
    /// that each specific follower needs based on their current high watermark.
    ///
    /// For each follower:
    /// - Filters entries to include only those the follower doesn't have
    /// - Sets correct previous log information based on follower's replication state:
    ///   - If follower needs all entries: Uses backup entry or defaults to (0,0)
    ///   - Otherwise: Uses the last entry the follower already has
    /// - Creates a tailored heartbeat message with exactly the entries needed
    ///
    /// Returns an iterator yielding tuples of mutable peer references and their
    /// customized heartbeat messages.
    async fn iter_follower_append_entries(
        &mut self,
    ) -> Box<dyn Iterator<Item = (&mut Peer, HeartBeat)> + '_> {
        let lowest_watermark = self.take_low_watermark();

        let append_entries = self.logger.list_append_log_entries(lowest_watermark).await;

        let default_heartbeat: HeartBeat = self.replication.default_heartbeat(
            0,
            self.logger.last_log_index,
            self.logger.last_log_term,
        );

        // Handle empty entries case
        if append_entries.is_empty() {
            return Box::new(
                self.replicas_mut().map(move |(peer, _)| (peer, default_heartbeat.clone())),
            );
        }

        // If we have entries, find the entry before the first one to use as backup
        // TODO perhaps we can get backup entry while getting append_entries?
        let backup_entry = self.logger.read_at(append_entries[0].log_index - 1).await;

        let iterator = self.replicas_mut().map(move |(peer, hwm)| {
            let logs =
                append_entries.iter().filter(|op| op.log_index > hwm).cloned().collect::<Vec<_>>();

            // Create base heartbeat
            let mut heart_beat = default_heartbeat.clone();

            if logs.len() == append_entries.len() {
                // Follower needs all entries, use backup entry
                if let Some(backup_entry) = backup_entry.as_ref() {
                    heart_beat.prev_log_index = backup_entry.log_index;
                    heart_beat.prev_log_term = backup_entry.term;
                } else {
                    heart_beat.prev_log_index = 0;
                    heart_beat.prev_log_term = 0;
                }
            } else {
                // Follower has some entries already, use the last one it has
                let last_log = &append_entries[append_entries.len() - logs.len() - 1];
                heart_beat.prev_log_index = last_log.log_index;
                heart_beat.prev_log_term = last_log.term;
            }
            let heart_beat = heart_beat.set_append_entries(logs);
            (peer, heart_beat)
        });

        Box::new(iterator)
    }

    pub(crate) fn take_low_watermark(&self) -> Option<u64> {
        self.members
            .values()
            .filter_map(|peer| match peer.kind() {
                | NodeKind::Replica => Some(peer.match_index()),
                | _ => None,
            })
            .min()
    }

    fn track_replication_progress(&mut self, res: ReplicationAck) {
        let Some(mut consensus) = self.consensus_tracker.remove(&res.log_idx) else {
            return;
        };

        if consensus.votable(&res.from) {
            info!("Received acks for log index num: {}", res.log_idx);
            if let Some(peer) = self.members.get_mut(&res.from) {
                peer.set_match_index(res.log_idx);
                peer.last_seen = Instant::now();
            }
            consensus.increase_vote(res.from);
        }
        if consensus.cnt < consensus.get_required_votes() {
            self.consensus_tracker.insert(res.log_idx, consensus);
            return;
        }

        // * Increase the high water mark
        self.replication.hwm.fetch_add(1, Ordering::Relaxed);

        self.client_sessions.set_response(consensus.session_req.take());
        let _ = consensus.callback.send(Ok(ConsensusClientResponse::LogIndex(res.log_idx.into())));
    }

    // After send_ack: Leader updates its knowledge of follower's progress
    async fn report_replica_lag(
        &mut self,
        send_to: &PeerIdentifier,
        log_idx: u64,
        rejection_reason: RejectionReason,
    ) {
        let Some(leader) = self.members.get_mut(send_to) else {
            return;
        };

        let _ =
            leader.send(ReplicationAck::new(log_idx, rejection_reason, &self.replication)).await;
    }

    async fn replicate(&mut self, mut heartbeat: HeartBeat, cache_manager: &CacheManager) {
        // * write logs
        if self.replicate_log_entries(&mut heartbeat).await.is_err() {
            error!("Failed to replicate logs");
            return;
        };
        self.replicate_state(heartbeat, cache_manager).await;
    }

    async fn replicate_log_entries(&mut self, rpc: &mut HeartBeat) -> anyhow::Result<()> {
        let entries = std::mem::take(&mut rpc.append_entries)
            .into_iter()
            .filter(|log| log.log_index > self.logger.last_log_index)
            .collect::<Vec<_>>();

        if entries.is_empty() {
            return Ok(());
        }

        if let Err(e) = self.ensure_prev_consistency(rpc.prev_log_index, rpc.prev_log_term).await {
            self.report_replica_lag(&rpc.from, self.logger.last_log_index, e).await;
            return Err(anyhow::anyhow!("Fail fail to append"));
        }

        let match_index = self.logger.follower_write_entries(entries).await?;

        self.report_replica_lag(&rpc.from, match_index, RejectionReason::None).await;
        Ok(())
    }

    async fn ensure_prev_consistency(
        &mut self,

        prev_log_index: u64,
        prev_log_term: u64,
    ) -> Result<(), RejectionReason> {
        // Case: Empty log
        if self.logger.is_empty() {
            if prev_log_index == 0 {
                return Ok(()); // First entry, no previous log to check
            }
            error!("Log is empty but leader expects an entry");
            return Err(RejectionReason::LogInconsistency); // Log empty but leader expects an entry
        }

        // * Raft followers should truncate their log starting at prev_log_index + 1 and then append the new entries
        // * Just returning an error is breaking consistency
        if let Some(prev_entry) = self.logger.read_at(prev_log_index).await {
            debug!("Previous log entry: {:?}", prev_entry);
            if prev_entry.term != prev_log_term {
                // ! Term mismatch -> triggers log truncation
                error!("Term mismatch: {} != {}", prev_entry.term, prev_log_term);
                self.logger.truncate_after(prev_log_index).await;

                return Err(RejectionReason::LogInconsistency);
            }
        }

        Ok(())
    }

    pub(crate) async fn send_rpc(&mut self) {
        if self.replicas().count() == 0 {
            return;
        }
        self.send_rpc_to_replicas().await;
    }

    pub(crate) fn cluster_nodes(&self) -> Vec<PeerState> {
        self.members
            .values()
            .map(|p| p.state().clone())
            .chain(std::iter::once(self.replication.self_info()))
            .collect()
    }

    pub(crate) async fn run_for_election(&mut self) {
        info!("Running for election term {}", self.replication.term);

        self.become_candidate();
        let request_vote = RequestVote::new(
            &self.replication,
            self.logger.last_log_index,
            self.logger.last_log_index,
        );

        self.replicas_mut()
            .map(|(peer, _)| peer.send(request_vote.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) async fn vote_election(&mut self, request_vote: RequestVote) {
        let grant_vote = self.logger.last_log_index <= request_vote.last_log_index
            && self.replication.become_follower_if_term_higher_and_votable(
                &request_vote.candidate_id,
                request_vote.term,
            );

        info!(
            "Voting for {} with term {} and granted: {}",
            request_vote.candidate_id, request_vote.term, grant_vote
        );

        let term = self.replication.term;
        let Some(peer) = self.find_replica_mut(&request_vote.candidate_id) else {
            return;
        };
        let _ = peer.send(ElectionVote { term, vote_granted: grant_vote }).await;
    }
    pub(crate) async fn ack_replication(&mut self, repl_res: ReplicationAck) {
        if !repl_res.is_granted() {
            self.handle_repl_rejection(repl_res).await;
            return;
        }
        self.update_on_hertbeat_message(&repl_res.from, repl_res.log_idx);
        self.track_replication_progress(repl_res);
    }
    pub(crate) async fn append_entries_rpc(
        &mut self,
        cache_manager: &CacheManager,
        heartbeat: crate::domains::peers::command::HeartBeat,
    ) {
        if self.check_term_outdated(&heartbeat).await {
            return;
        };
        self.reset_election_timeout(&heartbeat.from);
        self.maybe_update_term(heartbeat.term);
        self.replicate(heartbeat, cache_manager).await;
    }

    pub(crate) async fn receive_election_vote(&mut self, election_vote: ElectionVote) {
        if !election_vote.vote_granted {
            return;
        }
        if !self.replication.election_state.may_become_leader() {
            return;
        }
        self.become_leader().await;

        let msg = self.replication.default_heartbeat(
            0,
            self.logger.last_log_index,
            self.logger.last_log_term,
        );
        self.replicas_mut()
            .map(|(peer, _)| peer.send(QueryIO::AppendEntriesRPC(msg.clone())))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    fn reset_election_timeout(&mut self, leader_id: &PeerIdentifier) {
        if let Some(peer) = self.members.get_mut(leader_id) {
            peer.last_seen = Instant::now();
        }
        self.heartbeat_scheduler.reset_election_timeout();
        self.replication.election_state = ElectionState::Follower { voted_for: None };
    }

    async fn replicate_state(&mut self, leader_hwm: HeartBeat, cache_manager: &CacheManager) {
        let old_hwm = self.replication.hwm.load(Ordering::Acquire);
        if leader_hwm.hwm > old_hwm {
            debug!("Received commit offset {}", leader_hwm.hwm);

            for log_index in (old_hwm + 1)..=leader_hwm.hwm {
                let Some(log) = self.logger.read_at(log_index).await else {
                    self.report_replica_lag(
                        &leader_hwm.from,
                        self.logger.last_log_index,
                        RejectionReason::LogInconsistency,
                    )
                    .await;
                    error!("log has never been replicated!");
                    return;
                };

                if let Err(e) = cache_manager.apply_log(log.request, log_index).await {
                    // ! DON'T PANIC - post validation is where we just don't update state
                    error!("failed to apply log: {e}")
                }
            }
            self.replication.hwm.store(leader_hwm.hwm, Ordering::Release);
        }
    }

    fn maybe_update_term(&mut self, new_term: u64) {
        if new_term > self.replication.term {
            self.replication.term = new_term;
            self.replication.election_state = ElectionState::Follower { voted_for: None };
            self.replication.is_leader_mode = false;
            self.replication.role = ReplicationRole::Follower;
        }
    }

    async fn check_term_outdated(&mut self, heartbeat: &HeartBeat) -> bool {
        if heartbeat.term < self.replication.term {
            self.report_replica_lag(
                &heartbeat.from,
                self.logger.last_log_index,
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
        info!("\x1b[32mElection succeeded\x1b[0m");
        self.replication.become_leader();
        self.heartbeat_scheduler.turn_leader_mode().await;
    }
    fn become_candidate(&mut self) {
        let replica_count = self.replicas().count() as u8;
        self.replication.term += 1;
        self.replication.election_state.become_candidate(replica_count);
    }

    async fn handle_repl_rejection(&mut self, repl_res: ReplicationAck) {
        match repl_res.rej_reason {
            | RejectionReason::ReceiverHasHigherTerm => self.step_down().await,
            | RejectionReason::LogInconsistency => {
                eprintln!("Log inconsistency, reverting match index");
                //TODO we can refactor this to set match index to given log index from the follower
                self.decrease_match_index(&repl_res.from, repl_res.log_idx);
            },
            | RejectionReason::None => (),
        }
    }

    // TODO current_log_idx - 1?
    fn decrease_match_index(&mut self, from: &PeerIdentifier, current_log_idx: u64) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.set_match_index(current_log_idx);
        }
    }

    // * Forces the current node to become a replica of the given peer.
    pub(crate) async fn replicaof(
        &mut self,
        peer_addr: PeerIdentifier,
        callback: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    ) {
        self.logger.reset().await;
        self.replication.hwm.store(0, Ordering::Release);
        self.set_repl_id(ReplicationId::Undecided);
        self.step_down().await;

        let _ = self.connect_to_server(peer_addr, Some(callback)).await;
    }

    pub(crate) async fn cluster_meet(
        &mut self,
        peer_addr: PeerIdentifier,
        lazy_option: LazyOption,
        callback: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    ) {
        if !self.replication.is_leader_mode || self.replication.self_identifier() == peer_addr {
            let _ = callback.send(err!("wrong address or invalid state for cluster meet command"));
            return;
        }

        let _ = self.connect_to_server(peer_addr.clone(), Some(callback)).await;
        if lazy_option == LazyOption::Eager {
            self.request_rebalance(peer_addr).await;
        }
    }

    // Ask the given peer to act as rebalancing coordinator
    async fn request_rebalance(&mut self, peer_addr: PeerIdentifier) {
        // Block subsequent requests until the cluster is rebalanced
        self.pending_requests = Some(VecDeque::new());
        if let Some(peer) = self.members.get_mut(&peer_addr) {
            // let _ = peer.send(ClusterCommand::Rebalance).await;
        }
    }

    async fn join_peer_network_if_absent(&mut self, cluster_nodes: Vec<PeerState>) {
        let peers_to_connect = cluster_nodes
            .into_iter()
            .filter(|n| {
                let self_id = self.replication.self_identifier();
                // ! second condition is to avoid connection collisions
                n.addr != self_id && n.addr < self_id
            })
            .filter(|p| !self.members.contains_key(&p.addr))
            .filter(|p| !self.replication.in_ban_list(&p.addr))
            .map(|node| node.addr)
            .next();

        let Some(peer_to_connect) = peers_to_connect else {
            return;
        };

        let _ = self.connect_to_server(peer_to_connect, None).await;
    }
}

#[cfg(test)]
#[allow(unused_variables)]
pub mod test {
    use super::session::SessionRequest;
    use super::*;
    use crate::adapters::op_logs::memory_based::MemoryOpLogs;
    use crate::domains::caches::actor::CacheCommandSender;
    use crate::domains::caches::command::CacheCommand;

    use crate::domains::cluster_actors::replication::ReplicationRole;
    use crate::domains::operation_logs::WriteOperation;
    use crate::domains::operation_logs::WriteRequest;
    use crate::domains::peers::peer::PeerState;
    use crate::domains::peers::service::PeerListener;
    use std::ops::Range;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::fs::OpenOptions;

    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::channel;
    use tokio::time::timeout;
    use uuid::Uuid;

    fn write_operation_create_helper(
        index_num: u64,
        term: u64,
        key: &str,
        value: &str,
    ) -> WriteOperation {
        WriteOperation {
            log_index: index_num.into(),
            request: WriteRequest::Set { key: key.into(), value: value.into(), expires_at: None },
            term,
        }
    }
    fn heartbeat_create_helper(term: u64, hwm: u64, op_logs: Vec<WriteOperation>) -> HeartBeat {
        HeartBeat {
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

    pub async fn cluster_actor_create_helper() -> ClusterActor<MemoryOpLogs> {
        let replication = ReplicationState::new(
            ReplicationId::Key("master".into()),
            ReplicationRole::Leader,
            "localhost",
            8080,
            0,
        );
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("duva.tp");

        let topology_writer =
            OpenOptions::new().create(true).write(true).truncate(true).open(path).await.unwrap();

        ClusterActor::new(100, replication, 100, topology_writer, MemoryOpLogs::default())
    }

    async fn cluster_member_create_helper(
        actor: &mut ClusterActor<MemoryOpLogs>,
        num_stream: Range<u16>,
        cluster_sender: ClusterCommandHandler,
        cache_manager: CacheManager,
        follower_hwm: u64,
    ) {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind_addr = listener.local_addr().unwrap();

        for port in num_stream {
            let key = PeerIdentifier::new("localhost", port);
            let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();
            let kill_switch = PeerListener::spawn(r, cluster_sender.clone());
            actor.members.insert(
                PeerIdentifier::new("localhost", port),
                Peer::new(
                    x,
                    PeerState::new(
                        &format!("localhost:{}", port),
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
        let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 1);
        // THEN
        assert_eq!(hop_count, 0);
    }

    #[tokio::test]
    async fn test_hop_count_when_two() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 2);
        // THEN
        assert_eq!(hop_count, 0);
    }

    #[tokio::test]
    async fn test_hop_count_when_three() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 3);
        // THEN
        assert_eq!(hop_count, 1);
    }

    #[tokio::test]
    async fn test_hop_count_when_thirty() {
        // GIVEN
        let fanout = 2;

        // WHEN
        let hop_count = ClusterActor::<MemoryOpLogs>::hop_count(fanout, 30);
        // THEN
        assert_eq!(hop_count, 4);
    }

    #[tokio::test]
    async fn leader_consensus_tracker_not_changed_when_followers_not_exist() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;
        let (tx, rx) = tokio::sync::oneshot::channel();

        let consensus_request = ConsensusRequest::new(
            WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            tx,
            None,
        );

        // WHEN
        cluster_actor.req_consensus(consensus_request).await;

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(cluster_actor.logger.last_log_index, 1);
    }

    #[tokio::test]
    async fn req_consensus_inserts_consensus_voting() {
        // GIVEN

        let mut cluster_actor = cluster_actor_create_helper().await;

        // - add 5 followers
        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };

        cluster_member_create_helper(
            &mut cluster_actor,
            0..5,
            ClusterCommandHandler(cluster_sender),
            cache_manager,
            0,
        )
        .await;

        let (tx, _) = tokio::sync::oneshot::channel();
        let client_id = Uuid::now_v7();
        let session_request = SessionRequest::new(1, client_id);
        let consensus_request = ConsensusRequest::new(
            WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            tx,
            Some(session_request.clone()),
        );

        // WHEN
        cluster_actor.req_consensus(consensus_request).await;

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(cluster_actor.logger.last_log_index, 1);

        assert_eq!(
            cluster_actor.consensus_tracker.get(&1).unwrap().session_req.as_ref().unwrap().clone(), //* session_request_is_saved_on_tracker
            session_request
        );
    }

    #[tokio::test]
    async fn test_leader_req_consensus_early_return_when_already_processed_session_req_given() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;

        let cache_manager = CacheManager { inboxes: vec![] };

        let client_id = Uuid::now_v7();
        let client_req = SessionRequest::new(1, client_id);

        // WHEN - session request is already processed
        cluster_actor.client_sessions.set_response(Some(client_req.clone()));
        let handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(cache_manager));
        let (tx, rx) = tokio::sync::oneshot::channel();
        handler
            .send(ClusterCommand::Client(ClientMessage::LeaderReqConsensus(ConsensusRequest {
                request: WriteRequest::Set {
                    key: "foo".into(),
                    value: "bar".into(),
                    expires_at: None,
                },
                callback: tx,
                session_req: Some(client_req),
            })))
            .await
            .unwrap();

        // THEN
        rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_consensus_voting_deleted_when_consensus_reached() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        // - add followers to create quorum
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(
            &mut cluster_actor,
            0..4,
            ClusterCommandHandler(cluster_sender),
            cache_manager,
            0,
        )
        .await;
        let (client_request_sender, client_wait) = tokio::sync::oneshot::channel();

        let client_id = Uuid::now_v7();
        let client_request = SessionRequest::new(3, client_id);
        let consensus_request = ConsensusRequest::new(
            WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            client_request_sender,
            Some(client_request.clone()),
        );

        cluster_actor.req_consensus(consensus_request).await;

        // WHEN
        let follower_res = ReplicationAck {
            log_idx: 1,
            term: 0,
            rej_reason: RejectionReason::None,
            from: PeerIdentifier("".into()),
        };
        cluster_actor.track_replication_progress(follower_res.clone().set_from("repl1"));
        cluster_actor.track_replication_progress(follower_res.clone().set_from("repl2"));

        // up to this point, tracker hold the consensus
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(cluster_actor.consensus_tracker.get(&1).unwrap().voters.len(), 2);

        // ! Majority votes made
        cluster_actor.track_replication_progress(follower_res.set_from("repl3"));

        // THEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 0);
        assert_eq!(cluster_actor.logger.last_log_index, 1);

        client_wait.await.unwrap().unwrap();
        assert!(cluster_actor.client_sessions.is_processed(&Some(client_request))); // * session_request_is_marked_as_processed
    }

    #[tokio::test]
    async fn test_same_voter_can_vote_only_once() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);

        // - add followers to create quorum
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(
            &mut cluster_actor,
            0..4,
            ClusterCommandHandler(cluster_sender),
            cache_manager,
            0,
        )
        .await;
        let (client_request_sender, client_wait) = tokio::sync::oneshot::channel();

        let consensus_request = ConsensusRequest::new(
            WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            client_request_sender,
            None,
        );

        cluster_actor.req_consensus(consensus_request).await;

        // WHEN
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        let follower_res = ReplicationAck {
            log_idx: 1,
            term: 0,
            rej_reason: RejectionReason::None,
            from: PeerIdentifier("repl1".into()), //TODO Must be changed if "update_match_index" becomes idempotent operation on peer id
        };
        cluster_actor.track_replication_progress(follower_res.clone());
        cluster_actor.track_replication_progress(follower_res.clone());
        cluster_actor.track_replication_progress(follower_res.clone());

        // THEN - no change in consensus tracker even though the same voter voted multiple times
        assert_eq!(cluster_actor.consensus_tracker.len(), 1);
        assert_eq!(cluster_actor.logger.last_log_index, 1);
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
        let repl_state = ReplicationState::new(
            ReplicationId::Key("master".into()),
            ReplicationRole::Leader,
            "localhost",
            8080,
            0,
        );
        repl_state.hwm.store(LOWEST_FOLLOWER_COMMIT_INDEX, Ordering::Release);

        let log = &WriteRequest::Set { key: "foo4".into(), value: "bar".into(), expires_at: None };
        logger.write_single_entry(log, repl_state.term).await.unwrap();

        let logs = logger.list_append_log_entries(Some(LOWEST_FOLLOWER_COMMIT_INDEX)).await;

        // THEN
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 3);
        assert_eq!(logs[1].log_index, 4);
        assert_eq!(logger.last_log_index, 4);
    }

    #[tokio::test]
    async fn test_generate_follower_entries() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;

        let (cluster_sender, _) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(
            &mut cluster_actor,
            0..5,
            ClusterCommandHandler(cluster_sender.clone()),
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

        cluster_actor.logger.follower_write_entries(test_logs).await.unwrap();

        //WHEN
        // *add lagged followers with its commit index being 1
        let cache_manager = CacheManager { inboxes: vec![] };
        cluster_member_create_helper(
            &mut cluster_actor,
            5..7,
            ClusterCommandHandler(cluster_sender),
            cache_manager,
            1,
        )
        .await;

        // * add new log - this must create entries that are greater than 3
        let lowest_hwm = cluster_actor.take_low_watermark();

        cluster_actor
            .logger
            .write_single_entry(
                &WriteRequest::Set { key: "foo4".into(), value: "bar".into(), expires_at: None },
                cluster_actor.replication.term,
            )
            .await
            .unwrap();

        let entries = cluster_actor.iter_follower_append_entries().await.collect::<Vec<_>>();

        // * for old followers must have 1 entry
        assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 1).count(), 5);
        // * for lagged followers must have 3 entries
        assert_eq!(entries.iter().filter(|(_, hb)| hb.append_entries.len() == 3).count(), 2)
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_log() {
        // GIVEN
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
        cluster_actor.replicate(heartbeat, &cache_manager).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 0);
        assert_eq!(cluster_actor.logger.last_log_index, 2);
        let logs = cluster_actor.logger.range(0, 2).await;
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].log_index, 1);
        assert_eq!(logs[1].log_index, 2);
        assert_eq!(
            logs[0].request,
            WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None }
        );
        assert_eq!(
            logs[1].request,
            WriteRequest::Set { key: "foo2".into(), value: "bar".into(), expires_at: None }
        );
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state() {
        // GIVEN
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
        cluster_actor.replicate(heartbeat, &cache_manager).await;

        // WHEN - commit until 2
        let task = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    | CacheCommand::Set { cache_entry } => {
                        let (key, value) = cache_entry.destructure();
                        assert_eq!(value.value(), "bar");
                        if key == "foo2" {
                            break;
                        }
                    },
                    | _ => continue,
                }
            }
        });
        let heartbeat = heartbeat_create_helper(0, 2, vec![]);
        cluster_actor.replicate(heartbeat, &cache_manager).await;

        // THEN
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 2);
        assert_eq!(cluster_actor.logger.last_log_index, 2);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_multiple_committed_entries() {
        // GIVEN
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
        cluster_actor.replicate(heartbeat, &cache_manager).await;

        // Create a task to monitor applied entries
        let monitor_task = tokio::spawn(async move {
            let mut applied_keys = Vec::new();

            while let Some(message) = rx.recv().await {
                if let CacheCommand::Set { cache_entry } = message {
                    applied_keys.push(cache_entry.key().to_string());
                    if applied_keys.len() == 3 {
                        break;
                    }
                }
            }

            applied_keys
        });

        // WHEN - commit all entries
        let commit_heartbeat = heartbeat_create_helper(1, 3, vec![]);

        cluster_actor.replicate(commit_heartbeat, &cache_manager).await;

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
        let mut cluster_actor = cluster_actor_create_helper().await;

        // First, append some entries
        let first_entries = vec![
            write_operation_create_helper(1, 0, "key1", "value1"),
            write_operation_create_helper(2, 0, "key2", "value2"),
        ];

        let first_heartbeat = heartbeat_create_helper(1, 0, first_entries);

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let cache_manager = CacheManager { inboxes: vec![CacheCommandSender(tx)] };

        cluster_actor.replicate(first_heartbeat, &cache_manager).await;

        // Create a task to monitor applied entries
        let monitor_task = tokio::spawn(async move {
            let mut applied_keys = Vec::new();

            while let Some(message) = rx.recv().await {
                if let CacheCommand::Set { cache_entry } = message {
                    applied_keys.push(cache_entry.key().to_string());
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

        cluster_actor.replicate(second_heartbeat, &cache_manager).await;

        // THEN
        // Verify that only key1 was applied
        let applied_keys = tokio::time::timeout(Duration::from_secs(1), monitor_task)
            .await
            .expect("Timeout waiting for entries to be applied")
            .expect("Task failed");

        assert_eq!(applied_keys, vec!["key1"]);
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 1);
        assert_eq!(cluster_actor.logger.last_log_index, 3); // All entries are in the log
    }

    #[tokio::test]
    async fn follower_cluster_actor_replicate_state_only_upto_hwm() {
        // GIVEN
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
        cluster_actor.replicate(heartbeat, &cache_manager).await;

        // WHEN - commit only up to index 1
        let task = tokio::spawn(async move {
            let mut received_foo = false;

            while let Some(message) = rx.recv().await {
                match message {
                    | CacheCommand::Set { cache_entry } => {
                        let (key, value) = cache_entry.destructure();
                        if key == "foo" {
                            received_foo = true;
                            assert_eq!(value.value(), "bar");
                        } else if key == "foo2" {
                            // This should not happen in our test
                            panic!("foo2 should not be applied yet");
                        }
                    },
                    | _ => continue,
                }
            }

            received_foo
        });

        // Send a heartbeat with hwm=1 to commit only the first entry
        const HWM: u64 = 1;
        let heartbeat = heartbeat_create_helper(0, HWM, vec![]);
        cluster_actor.replicate(heartbeat, &cache_manager).await;

        // THEN
        // Give the task a chance to process the message
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the task since we're done checking
        task.abort();

        // Verify state
        assert_eq!(cluster_actor.replication.hwm.load(Ordering::Relaxed), 1);
        assert_eq!(cluster_actor.logger.last_log_index, 2);
    }

    #[tokio::test]
    async fn follower_truncates_log_on_term_mismatch() {
        // GIVEN: A follower with an existing log entry at index  term 1
        let mut inmemory = MemoryOpLogs::default();
        //prefill

        inmemory.writer.extend(vec![
            write_operation_create_helper(2, 1, "key1", "val1"),
            write_operation_create_helper(3, 1, "key2", "val2"),
        ]);

        let logger = ReplicatedLogs::new(inmemory, 3, 1);

        let mut cluster_actor = cluster_actor_create_helper().await;
        cluster_actor.logger = logger;

        // Simulate an initial log entry at index 1, term 1
        // WHEN: Leader sends an AppendEntries with prev_log_index=1, prev_log_term=2 (mismatch)
        let mut heartbeat = heartbeat_create_helper(
            2,
            0,
            vec![write_operation_create_helper(4, 0, "key2", "val2")],
        );
        heartbeat.prev_log_term = 0;
        heartbeat.prev_log_index = 2;

        let result = cluster_actor.replicate_log_entries(&mut heartbeat).await;

        // THEN: Expect truncation and rejection
        assert_eq!(cluster_actor.logger.target.writer.len(), 1);
        assert!(result.is_err(), "Should reject due to term mismatch");
    }

    #[tokio::test]
    async fn follower_accepts_entries_with_empty_log_and_prev_log_index_zero() {
        // GIVEN: A follower with an empty log

        let mut cluster_actor = cluster_actor_create_helper().await;

        // WHEN: Leader sends entries with prev_log_index=0
        let mut heartbeat = heartbeat_create_helper(
            1,
            0,
            vec![write_operation_create_helper(1, 0, "key1", "val1")],
        );

        let result = cluster_actor.replicate_log_entries(&mut heartbeat).await;

        // THEN: Entries are accepted
        assert!(result.is_ok(), "Should accept entries with prev_log_index=0 on empty log");
        assert_eq!(cluster_actor.logger.last_log_index, 1); // Assuming write_log_entries updates log_index
    }

    #[tokio::test]
    async fn follower_rejects_entries_with_empty_log_and_prev_log_index_nonzero() {
        // GIVEN: A follower with an empty log

        let mut cluster_actor = cluster_actor_create_helper().await;

        // WHEN: Leader sends entries with prev_log_index=1
        let mut heartbeat = heartbeat_create_helper(
            1,
            0,
            vec![write_operation_create_helper(2, 0, "key2", "val2")],
        );
        heartbeat.prev_log_index = 1;
        heartbeat.prev_log_term = 1;

        let result = cluster_actor.replicate_log_entries(&mut heartbeat).await;

        // THEN: Entries are rejected
        assert!(result.is_err(), "Should reject entries with prev_log_index > 0 on empty log");
        assert_eq!(cluster_actor.logger.last_log_index, 0); // Log should remain unchanged
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
        cluster_actor.replication.hwm.store(15, Ordering::Release);

        let repl_id = cluster_actor.replication.replid.clone();

        // followers
        for port in [6379, 6380] {
            let key = PeerIdentifier::new("127.0.0.1", port);
            let (r, x) = TcpStream::connect(bind_addr).await.unwrap().into_split();
            let kill_switch = PeerListener::spawn(r, ClusterCommandHandler(cluster_sender.clone()));
            cluster_actor.members.insert(
                key.clone(),
                Peer::new(
                    x,
                    PeerState::new(
                        &key,
                        cluster_actor.replication.hwm.load(Ordering::Relaxed),
                        repl_id.clone(),
                        NodeKind::Replica,
                    ),
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
        let kill_switch = PeerListener::spawn(r, ClusterCommandHandler(cluster_sender.clone()));

        cluster_actor.members.insert(
            second_shard_leader_identifier.clone(),
            Peer::new(
                x,
                PeerState::new(
                    &second_shard_leader_identifier,
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
            let kill_switch = PeerListener::spawn(r, ClusterCommandHandler(cluster_sender.clone()));

            cluster_actor.members.insert(
                key.clone(),
                Peer::new(
                    x,
                    PeerState::new(
                        &key,
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
        127.0.0.1:6379 {repl_id} 0 15
        127.0.0.1:6380 {repl_id} 0 15
        {second_shard_leader_identifier} {second_shard_repl_id} 0 0
        127.0.0.1:2655 {second_shard_repl_id} 0 0
        127.0.0.1:2653 {second_shard_repl_id} 0 0
        localhost:8080 myself,{repl_id} 0 15
        "#
        );
        let mut temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
        write!(temp_file, "{}", file_content).expect("Failed to write to temp file");
        let nodes = PeerState::from_file(temp_file.path().to_str().unwrap());

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
        let hwm = cluster_actor.replication.hwm.load(Ordering::Relaxed);

        // WHEN
        cluster_actor.snapshot_topology().await.unwrap();

        // THEN
        let topology = tokio::fs::read_to_string(path).await.unwrap();
        let expected_topology = format!("{} myself,{} 0 {}", self_id, repl_id, hwm);
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

        let kill_switch = PeerListener::spawn(r, cluster_actor.self_handler.clone());

        let peer = Peer::new(
            x,
            PeerState::new(
                "127.0.0.1:3849",
                0,
                ReplicationId::Key(repl_id.to_string()),
                NodeKind::Replica,
            ),
            kill_switch,
        );

        // WHEN
        cluster_actor.add_peer(peer).await;
        cluster_actor.snapshot_topology().await.unwrap();

        // THEN
        let topology = tokio::fs::read_to_string(path).await.unwrap();
        let mut cluster_nodes =
            topology.split("\r\n").map(|x| x.to_string()).collect::<Vec<String>>();

        cluster_nodes.dedup();
        assert_eq!(cluster_nodes.len(), 2);

        let hwm = cluster_actor.replication.hwm.load(Ordering::Relaxed);

        for value in [
            format!("127.0.0.1:3849 {} 0 {}", repl_id, hwm),
            format!("{} myself,{} 0 {}", cluster_actor.replication.self_identifier(), repl_id, hwm),
        ] {
            assert!(cluster_nodes.contains(&value));
        }

        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn test_requests_pending() {
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;
        cluster_actor.pending_requests = Some(Default::default());

        //WHEN
        let (tx, rx) = tokio::sync::oneshot::channel();
        let write_r =
            WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None };
        let con_req = ConsensusRequest::new(write_r.clone(), tx, None);

        cluster_actor.req_consensus(con_req).await;

        // THEN
        assert!(timeout(Duration::from_millis(200), rx).await.is_err());
        assert_eq!(cluster_actor.pending_requests.as_ref().unwrap().len(), 1);
        assert_eq!(
            cluster_actor.pending_requests.as_mut().unwrap().pop_front().unwrap().request,
            write_r
        );
    }

    #[tokio::test]
    async fn test_reconnection_on_gossip() {
        use crate::domains::cluster_actors::actor::test::cluster_actor_create_helper;
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
            .join_peer_network_if_absent(vec![PeerState::new(
                &format!("127.0.0.1:{}", bind_addr.port() - 10000),
                0,
                cluster_actor.replication.replid.clone(),
                NodeKind::Replica,
            )])
            .await;

        assert!(handle.await.is_ok());
        assert!(rx.await.is_ok());
    }
}
