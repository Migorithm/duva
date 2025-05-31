use super::ClusterCommand;
use super::ConsensusClientResponse;
use super::ConsensusRequest;
use super::LazyOption;
use super::consensus::election::ElectionState;
use super::hash_ring::HashRing;
use super::heartbeat_scheduler::HeartBeatScheduler;
use super::replication::ReplicationId;
use super::replication::ReplicationRole;
use super::replication::ReplicationState;
use super::replication::time_in_secs;
use super::session::ClientSessions;
use super::*;

use crate::domains::caches::cache_manager::CacheManager;

use crate::domains::QueryIO;
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
use tracing::instrument;
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
    pub(crate) hash_ring: HashRing,
}

#[derive(Debug, Clone)]
pub struct ClusterCommandHandler(pub(super) tokio::sync::mpsc::Sender<ClusterCommand>);
impl ClusterCommandHandler {
    pub(crate) async fn send(
        &self,
        cmd: impl Into<ClusterCommand>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ClusterCommand>> {
        self.0.send(cmd.into()).await
    }
}

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(crate) fn run(
        node_timeout: u128,
        topology_writer: tokio::fs::File,
        heartbeat_interval: u64,
        init_replication: ReplicationState,
        cache_manager: CacheManager,
        wal: T,
    ) -> ClusterCommandHandler {
        let cluster_actor = ClusterActor::new(
            node_timeout,
            init_replication,
            heartbeat_interval,
            topology_writer,
            wal,
        );
        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(cache_manager));
        actor_handler
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, peer),fields(peer_id = %peer.id()))]
    pub(crate) async fn add_peer(&mut self, peer: Peer) {
        self.replication.banlist.remove(peer.id());

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer.id().clone(), peer) {
            existing_peer.kill().await;
        }

        self.broadcast_topology_change();
        let _ = self.snapshot_topology().await;
    }

    #[instrument(skip(self, optional_callback))]
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

    pub(crate) fn accept_inbound_stream(&mut self, peer_stream: TcpStream) {
        let inbound_stream = InboundStream::new(peer_stream, self.replication.clone());
        tokio::spawn(inbound_stream.add_peer(self.self_handler.clone()));
    }

    pub(crate) fn set_repl_id(&mut self, leader_repl_id: ReplicationId) {
        self.replication.replid = leader_repl_id;
    }

    pub(super) fn new(
        node_timeout: u128,
        init_repl_state: ReplicationState,
        heartbeat_interval_in_mills: u64,
        topology_writer: File,
        log_writer: T,
    ) -> Self {
        let (self_handler, receiver) = tokio::sync::mpsc::channel(100);
        let heartbeat_scheduler = HeartBeatScheduler::run(
            self_handler.clone(),
            init_repl_state.is_leader_mode,
            heartbeat_interval_in_mills,
        );

        let (tx, _) = tokio::sync::broadcast::channel::<Vec<PeerIdentifier>>(100);
        let mut hash_ring = HashRing::default();
        hash_ring.add_partition_if_not_exists(
            init_repl_state.replid.clone(),
            init_repl_state.self_identifier(),
        );

        Self {
            heartbeat_scheduler,
            replication: init_repl_state,
            node_timeout,
            receiver,
            self_handler: ClusterCommandHandler(self_handler),
            topology_writer,
            node_change_broadcast: tx,
            hash_ring,
            members: BTreeMap::new(),
            consensus_tracker: LogConsensusTracker::default(),
            client_sessions: ClientSessions::default(),

            // todo initial value setting
            logger: ReplicatedLogs::new(log_writer, 0, 0),
            pending_requests: None,
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    pub(crate) async fn send_cluster_heartbeat(&mut self) {
        self.remove_idle_peers().await;

        let hop_count = Self::hop_count(FANOUT, self.members.len());
        let hb = self
            .replication
            .default_heartbeat(hop_count, self.logger.last_log_index, self.logger.last_log_term)
            .set_cluster_nodes(self.cluster_nodes());
        self.send_heartbeat(hb).await;
    }

    pub(crate) async fn forget_peer(
        &mut self,
        peer_addr: PeerIdentifier,
    ) -> anyhow::Result<Option<()>> {
        let res = self.remove_peer(&peer_addr).await;
        self.replication.banlist.insert(BannedPeer { p_id: peer_addr, ban_time: time_in_secs()? });

        Ok(res)
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, heartbeat), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn receive_cluster_heartbeat(&mut self, mut heartbeat: HeartBeat) {
        if self.replication.in_ban_list(&heartbeat.from) {
            return;
        }
        self.apply_banlist(std::mem::take(&mut heartbeat.ban_list)).await;
        self.join_peer_network_if_absent(heartbeat.cluster_nodes).await;
        self.gossip(heartbeat.hop_count).await;
        self.update_on_hertbeat_message(&heartbeat.from, heartbeat.hwm);
        self.make_migration_plan_if_valid(heartbeat.hashring).await;
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

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
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
    #[instrument(level = tracing::Level::INFO, skip(self, request_vote))]
    pub(crate) async fn vote_election(&mut self, request_vote: RequestVote) {
        let grant_vote = self.logger.last_log_index <= request_vote.last_log_index
            && self.replication.become_follower_if_term_higher_and_votable(
                &request_vote.candidate_id,
                request_vote.term,
            );

        info!(
            "Voting for {} with term {} and granted: {grant_vote}",
            request_vote.candidate_id, request_vote.term
        );

        let term = self.replication.term;
        let Some(peer) = self.find_replica_mut(&request_vote.candidate_id) else {
            return;
        };
        let _ = peer.send(ElectionVote { term, vote_granted: grant_vote }).await;
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, repl_res), fields(peer_id = %repl_res.from))]
    pub(crate) async fn ack_replication(&mut self, repl_res: ReplicationAck) {
        if !repl_res.is_granted() {
            self.handle_repl_rejection(repl_res).await;
            return;
        }
        self.update_on_hertbeat_message(&repl_res.from, repl_res.log_idx);
        self.track_replication_progress(repl_res);
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, cache_manager,heartbeat), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn append_entries_rpc(
        &mut self,
        cache_manager: &CacheManager,
        heartbeat: HeartBeat,
    ) {
        if self.check_term_outdated(&heartbeat).await {
            return;
        };
        self.reset_election_timeout(&heartbeat.from);
        self.maybe_update_term(heartbeat.term);
        self.replicate(heartbeat, cache_manager).await;
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, election_vote))]
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
        self.connect_to_server(peer_addr, Some(callback)).await;
    }

    pub(crate) async fn cluster_meet(
        &mut self,
        peer_addr: PeerIdentifier,
        lazy_option: LazyOption,
        cl_cb: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    ) {
        if !self.replication.is_leader_mode || self.replication.self_identifier() == peer_addr {
            let _ = cl_cb.send(err!("wrong address or invalid state for cluster meet command"));
            return;
        }

        // ! intercept the callback to ensure that the connection is established before sending the rebalance request
        let (res_callback, conn_awaiter) = tokio::sync::oneshot::channel();
        self.connect_to_server(peer_addr.clone(), Some(res_callback)).await;

        tokio::spawn(Self::register_delayed_schedule(
            self.self_handler.clone(),
            conn_awaiter,
            cl_cb,
            SchedulerMessage::RebalanceRequest { request_to: peer_addr, lazy_option },
        ));
    }

    pub(crate) async fn rebalance_request(
        &mut self,
        request_to: PeerIdentifier,
        lazy_option: LazyOption,
    ) {
        // * If lazy option is set, we just send the request and don't wait for the response
        // Ask the given peer to act as rebalancing coordinator
        if lazy_option == LazyOption::Eager {
            let Some(peer) = self.members.get(&request_to) else {
                return;
            };
            if peer.is_replica() {
                warn!("Cannot rebalance to a replica: {}", request_to);
                return;
            }

            self.block_write_reqs();

            let peer = self.members.get_mut(&request_to).unwrap();
            let _ = peer.send(QueryIO::StartRebalance).await;
        }
    }

    #[instrument(level = tracing::Level::INFO, skip(self), fields(request_from = %request_from))]
    pub(crate) async fn start_rebalance(&mut self, request_from: PeerIdentifier) {
        let Some(member) = self.members.get(&request_from) else {
            error!("Received rebalance request from unknown peer: {}", request_from);
            return;
        };

        if member.is_replica() {
            error!("Cannot rebalance to a replica: {}", request_from);
            return;
        }

        if self
            .hash_ring
            .add_partition_if_not_exists(member.replid().clone(), member.id().clone())
            .is_some()
        {
            warn!("Rebalancing started! subsequent writes will be blocked until rebalance is done");
            self.block_write_reqs();
        };

        let hb = self
            .replication
            .default_heartbeat(
                Self::hop_count(FANOUT, self.members.len()),
                self.logger.last_log_index,
                self.logger.last_log_term,
            )
            .set_hashring(self.hash_ring.clone());

        self.send_heartbeat(hb).await;
    }

    fn hop_count(fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }

    fn replicas(&self) -> impl Iterator<Item = (&PeerIdentifier, u64)> {
        self.members.iter().filter_map(|(id, peer)| {
            (peer.kind() == &NodeKind::Replica).then_some((id, peer.match_index()))
        })
    }

    fn replicas_mut(&mut self) -> impl Iterator<Item = (&mut Peer, u64)> {
        self.members.values_mut().filter_map(|peer| {
            let match_index = peer.match_index();
            (peer.kind() == &NodeKind::Replica).then_some((peer, match_index))
        })
    }

    fn find_replica_mut(&mut self, peer_id: &PeerIdentifier) -> Option<&mut Peer> {
        self.members.get_mut(peer_id).filter(|peer| peer.kind() == &NodeKind::Replica)
    }

    async fn send_heartbeat(&mut self, heartbeat: HeartBeat) {
        for peer in self.members.values_mut() {
            let _ = peer.send(heartbeat.clone()).await;
        }
    }

    async fn snapshot_topology(&mut self) -> anyhow::Result<()> {
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

    // ! BLOCK subsequent requests until rebalance is done
    fn block_write_reqs(&mut self) {
        if self.pending_requests.is_none() {
            self.pending_requests = Some(VecDeque::new());
        }
    }

    // * Broadcasts the current topology to all connected clients
    // TODO hashring information should be included in the broadcast so clients can update their routing tables
    fn broadcast_topology_change(&self) {
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
            self.broadcast_topology_change();
            return Some(());
        }
        None
    }

    //  remove idle peers based on ttl.
    async fn remove_idle_peers(&mut self) {
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

        let hb = self
            .replication
            .default_heartbeat(hop_count, self.logger.last_log_index, self.logger.last_log_term)
            .set_cluster_nodes(self.cluster_nodes());
        self.send_heartbeat(hb).await;
    }

    async fn apply_banlist(&mut self, ban_list: Vec<BannedPeer>) {
        for banned_peer in ban_list {
            let ban_list = &mut self.replication.banlist;

            if let Some(existing) = ban_list.take(&banned_peer) {
                let newer =
                    if banned_peer.ban_time > existing.ban_time { banned_peer } else { existing };
                ban_list.insert(newer);
            } else {
                ban_list.insert(banned_peer);
            }
        }

        let current_time_in_sec = time_in_secs().unwrap();
        self.replication.banlist.retain(|node| current_time_in_sec - node.ban_time < 60);
        for banned_peer in self.replication.banlist.iter().cloned().collect::<Vec<_>>() {
            self.remove_peer(&banned_peer.p_id).await;
        }
    }

    fn update_on_hertbeat_message(&mut self, from: &PeerIdentifier, log_index: u64) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.last_seen = Instant::now();
            peer.set_match_index(log_index);
        }
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

    fn take_low_watermark(&self) -> Option<u64> {
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

    #[instrument(level = tracing::Level::INFO, skip(self))]
    pub(crate) async fn run_for_election(&mut self) {
        warn!("Running for election term {}", self.replication.term);

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
                    error!("log has never been replicated!");
                    self.report_replica_lag(
                        &leader_hwm.from,
                        self.logger.last_log_index,
                        RejectionReason::LogInconsistency,
                    )
                    .await;
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
    async fn step_down(&mut self) {
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

    fn decrease_match_index(&mut self, from: &PeerIdentifier, current_log_idx: u64) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.set_match_index(current_log_idx);
        }
    }

    async fn register_delayed_schedule<C>(
        cluster_sender: ClusterCommandHandler,
        awaiter: tokio::sync::oneshot::Receiver<anyhow::Result<C>>,
        callback: tokio::sync::oneshot::Sender<anyhow::Result<C>>,
        schedule_cmd: SchedulerMessage,
    ) where
        C: Send + Sync + 'static,
    {
        let result = awaiter.await.unwrap_or_else(|_| Err(anyhow::anyhow!("Channel closed")));
        let _ = callback.send(result);
        // Send schedule command regardless of callback result
        if let Err(e) = cluster_sender.send(schedule_cmd).await {
            // Consider logging this error instead of silently ignoring
            error!("Failed to send schedule command: {}", e);
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

        self.connect_to_server(peer_to_connect, None).await;
    }

    async fn make_migration_plan_if_valid(&self, hashring: Option<HashRing>) {
        let Some(ring) = hashring else {
            return;
        };
        if ring == self.hash_ring {
            return;
        }
        if ring.last_modified < self.hash_ring.last_modified {
            warn!("Received outdated hashring, ignoring");
            return;
        }
    }
}

#[cfg(test)]
pub mod cluster_actor_setups {
    use std::sync::Arc;

    use crate::{
        adapters::op_logs::memory_based::MemoryOpLogs,
        domains::{IoError, TRead, TWrite, peers::service::PeerListener},
        make_smart_pointer,
    };

    use super::*;
    use bytes::BytesMut;
    use tokio::sync::Mutex;
    #[derive(Debug, Clone)]
    pub struct FakeReadWrite(Arc<Mutex<VecDeque<QueryIO>>>);
    make_smart_pointer!(FakeReadWrite, Arc<Mutex<VecDeque<QueryIO>>>);

    impl FakeReadWrite {
        pub fn new() -> Self {
            Self(Arc::new(Mutex::new(VecDeque::new())))
        }
    }
    #[async_trait::async_trait]
    impl TWrite for FakeReadWrite {
        async fn write(&mut self, io: QueryIO) -> Result<(), IoError> {
            let mut guard = self.0.lock().await;
            guard.push_back(io);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl TRead for FakeReadWrite {
        async fn read_bytes(&mut self, _buf: &mut BytesMut) -> Result<(), IoError> {
            Ok(())
        }

        async fn read_values(&mut self) -> Result<Vec<QueryIO>, IoError> {
            let mut values = Vec::new();
            let mut guard = self.0.lock().await;

            values.extend(guard.drain(..));
            Ok(values)
        }
    }

    pub(crate) fn create_peer_helper(
        cluster_sender: ClusterCommandHandler,
        hwm: u64,
        repl_id: &ReplicationId,
        port: u16,
        node_kind: NodeKind,
        fake_buf: FakeReadWrite,
    ) -> (PeerIdentifier, Peer) {
        let key = PeerIdentifier::new("127.0.0.1", port);

        let kill_switch = PeerListener::spawn(fake_buf.clone(), cluster_sender, key.clone());
        let peer =
            Peer::new(fake_buf, PeerState::new(&key, hwm, repl_id.clone(), node_kind), kill_switch);
        (key, peer)
    }

    #[cfg(test)]
    impl<T: TWriteAheadLog> ClusterActor<T> {
        pub(crate) fn test_add_peer(
            &mut self,
            port: u16,
            kind: NodeKind,
            repl_id: Option<ReplicationId>,
        ) -> (FakeReadWrite, PeerIdentifier) {
            let buf = FakeReadWrite::new();
            let (id, peer) = create_peer_helper(
                self.self_handler.clone(),
                0,
                &repl_id.unwrap_or_else(|| self.replication.replid.clone()),
                port,
                kind,
                buf.clone(),
            );

            self.members.insert(id.clone(), peer);
            (buf, id)
        }
        // COPY of block_write_reqs for testing
        pub(crate) fn test_block_write_reqs(&mut self) {
            self.block_write_reqs();
        }

        pub(crate) fn test_track_replication_progress(&mut self, res: ReplicationAck) {
            self.track_replication_progress(res);
        }

        pub(crate) async fn test_replicate_log_entries(
            &mut self,
            rpc: &mut HeartBeat,
        ) -> anyhow::Result<()> {
            self.replicate_log_entries(rpc).await
        }

        pub(crate) async fn test_replicate(
            &mut self,
            heartbeat: HeartBeat,
            cache_manager: &CacheManager,
        ) {
            self.replicate(heartbeat, cache_manager).await;
        }
        pub(crate) async fn test_join_peer_network_if_absent(
            &mut self,
            cluster_nodes: Vec<PeerState>,
        ) {
            self.join_peer_network_if_absent(cluster_nodes).await;
        }
        pub(crate) async fn test_snapshot_topology(&mut self) -> anyhow::Result<()> {
            self.snapshot_topology().await
        }

        pub(crate) async fn test_iter_follower_append_entries(
            &mut self,
        ) -> Box<dyn Iterator<Item = (&mut Peer, HeartBeat)> + '_> {
            self.iter_follower_append_entries().await
        }

        pub(crate) async fn test_make_migration_plan_if_valid(&self, hashring: Option<HashRing>) {
            self.make_migration_plan_if_valid(hashring).await;
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
}
