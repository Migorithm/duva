use super::ClusterCommand;
use super::ConsensusClientResponse;
use super::ConsensusRequest;
use super::LazyOption;
use super::consensus::election::ElectionState;
use super::hash_ring::HashRing;
pub mod client_sessions;
pub(crate) mod heartbeat_scheduler;
use super::replication::ReplicationId;
use super::replication::ReplicationRole;
use super::replication::ReplicationState;
use super::replication::time_in_secs;
use super::*;
use crate::domains::QueryIO;
use crate::domains::TAsyncReadWrite;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::consensus::LogConsensusVoting;
use crate::domains::cluster_actors::consensus::election::ElectionVoting;
use crate::domains::cluster_actors::consensus::election::REQUESTS_BLOCKED_BY_ELECTION;
use crate::domains::cluster_actors::queue::ClusterActorQueue;
use crate::domains::cluster_actors::queue::ClusterActorReceiver;
use crate::domains::cluster_actors::queue::ClusterActorSender;
use crate::domains::cluster_actors::topology::{NodeReplInfo, Topology};
use crate::domains::operation_logs::LogEntry;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::BatchEntries;
use crate::domains::peers::command::BatchId;
use crate::domains::peers::command::ElectionVote;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::InProgressMigration;
use crate::domains::peers::command::PendingMigrationTask;
use crate::domains::peers::command::QueuedKeysToMigrate;
use crate::domains::peers::command::RejectionReason;
use crate::domains::peers::command::ReplicationAck;
use crate::domains::peers::command::RequestVote;
use crate::domains::peers::connections::connection_types::ReadConnected;
use crate::domains::peers::connections::connection_types::WriteConnected;
use crate::domains::peers::connections::inbound::stream::InboundStream;
use crate::domains::peers::connections::outbound::stream::OutboundStream;
use crate::domains::peers::identifier::TPeerAddress;
use crate::domains::peers::peer::PeerState;
use crate::err;
use crate::res_err;
use crate::types::Callback;
use crate::types::CallbackAwaiter;
use client_sessions::ClientSessions;
use heartbeat_scheduler::HeartBeatScheduler;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::io::Seek;
use std::io::Write;
use std::iter;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct ClusterActor<T> {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationState<T>,
    pub(crate) consensus_tracker: LogConsensusTracker,
    pub(crate) receiver: ClusterActorReceiver,
    pub(crate) self_handler: ClusterActorSender,
    pub(crate) heartbeat_scheduler: HeartBeatScheduler,
    pub(crate) topology_writer: std::fs::File,
    pub(crate) node_change_broadcast: tokio::sync::broadcast::Sender<Topology>,
    pub(crate) cache_manager: CacheManager,

    // * Pending requests are used to store requests that are received while the actor is in the process of election/cluster rebalancing.
    // * These requests will be processed once the actor is back to a stable state.
    pub(crate) client_sessions: ClientSessions,
    pub(crate) hash_ring: HashRing,
    migrations_in_progress: Option<InProgressMigration>,
    cluster_join_sync: ClusterJoinSync,
}

#[derive(Debug, Default)]
struct ClusterJoinSync {
    known_peers: HashMap<PeerIdentifier, bool>,
    notifier: Option<Callback<()>>,
    waiter: Option<CallbackAwaiter<()>>,
}
impl ClusterJoinSync {
    fn new(known_peers: Vec<PeerIdentifier>) -> Self {
        let (notifier, waiter) = Callback::create();
        Self {
            notifier: Some(notifier),
            waiter: Some(waiter),
            known_peers: known_peers.into_iter().map(|p| (p, true)).collect(),
        }
    }
}

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(crate) fn run(
        topology_writer: std::fs::File,
        heartbeat_interval: u64,
        init_repl_state: ReplicationState<T>,
        cache_manager: CacheManager,
    ) -> ClusterActorSender {
        let cluster_actor =
            ClusterActor::new(init_repl_state, heartbeat_interval, topology_writer, cache_manager);
        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle());
        actor_handler
    }

    fn logger(&self) -> &ReplicatedLogs<T> {
        &self.replication.logger
    }

    fn new(
        init_repl_state: ReplicationState<T>,
        heartbeat_interval_in_mills: u64,
        topology_writer: File,
        cache_manager: CacheManager,
    ) -> Self {
        let (self_handler, receiver) = ClusterActorQueue::create(2000);
        let heartbeat_scheduler = HeartBeatScheduler::run(
            self_handler.clone(),
            init_repl_state.is_leader(),
            heartbeat_interval_in_mills,
        );

        let (tx, _) = tokio::sync::broadcast::channel::<Topology>(100);
        let hash_ring = HashRing::default().add_partitions(vec![(
            init_repl_state.replid.clone(),
            init_repl_state.self_identifier(),
        )]);

        Self {
            heartbeat_scheduler,
            replication: init_repl_state,
            receiver,
            self_handler,
            topology_writer,
            node_change_broadcast: tx,
            hash_ring,
            members: BTreeMap::new(),
            consensus_tracker: LogConsensusTracker::default(),
            client_sessions: ClientSessions::default(),
            cluster_join_sync: ClusterJoinSync::default(),
            migrations_in_progress: None,
            cache_manager,
        }
    }

    #[instrument(level = tracing::Level::INFO, skip(self, peer, optional_callback),fields(peer_id = %peer.id()))]
    pub(crate) async fn add_peer(
        &mut self,
        peer: Peer,
        optional_callback: Option<Callback<anyhow::Result<()>>>,
    ) {
        let peer_id = peer.id().clone();
        self.replication.banlist.remove(&peer_id);

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer_id.clone(), peer) {
            existing_peer.kill().await;
        }

        self.broadcast_topology_change();
        let _ = self.snapshot_topology().await;

        let peer = self.members.get_mut(&peer_id).unwrap();
        if peer.is_follower(&self.replication.replid) && self.replication.is_leader() {
            info!("Sending heartbeat to newly added follower: {}", peer_id);
            let hb = self.replication.default_heartbeat(0).set_hashring(self.hash_ring.clone());
            let _ = peer.send(hb).await;
        }

        if let Some(cb) = optional_callback {
            cb.send(Ok(()));
        }

        self.cluster_join_sync.known_peers.entry(peer_id).and_modify(|e| *e = true);
        self.signal_if_peer_discovery_complete();
    }

    pub(crate) fn activate_cluster_sync(&mut self, callback: Callback<()>) {
        self.cluster_join_sync = ClusterJoinSync::new(self.members.keys().cloned().collect());
        callback.send(());
    }
    pub(crate) fn send_cluster_sync_awaiter(
        &mut self,
        callback: Callback<Option<CallbackAwaiter<()>>>,
    ) {
        callback.send(self.cluster_join_sync.waiter.take());
    }

    #[instrument(skip(self, optional_callback))]
    pub(crate) async fn connect_to_server<C: TAsyncReadWrite>(
        &mut self,
        connect_to: PeerIdentifier,
        optional_callback: Option<Callback<anyhow::Result<()>>>,
    ) {
        if self.replication.self_identifier() == connect_to {
            if let Some(cb) = optional_callback {
                cb.send(res_err!("Cannot connect to self"));
            }
            return;
        }

        let Ok(cluster_bind_addr) = connect_to.cluster_bind_addr() else {
            if let Some(cb) = optional_callback {
                cb.send(res_err!("invalid cluster bind address for {}", connect_to));
            }
            return;
        };

        let Ok((r, w)) = C::connect(&cluster_bind_addr).await else {
            if let Some(cb) = optional_callback {
                cb.send(res_err!("Failed to connect to {}", connect_to));
            }
            return;
        };

        let outbound_stream =
            OutboundStream { r, w, self_repl_info: self.replication.info(), peer_state: None };

        tokio::spawn(outbound_stream.add_peer(
            self.replication.self_port,
            self.self_handler.clone(),
            optional_callback,
        ));
    }

    pub(crate) fn accept_inbound_stream(
        &mut self,
        r: ReadConnected,
        w: WriteConnected,
        host_ip: String,
    ) {
        let inbound_stream = InboundStream {
            r,
            w,
            host_ip,
            self_repl_info: self.replication.info(),
            peer_state: Default::default(),
        };

        tokio::spawn(inbound_stream.add_peer(self.self_handler.clone()));
    }

    #[instrument(level = tracing::Level::INFO, skip(self,replid))]
    pub(crate) fn follower_setup(&mut self, replid: ReplicationId) {
        self.set_repl_id(replid.clone());
    }

    pub(crate) fn set_repl_id(&mut self, replid: ReplicationId) {
        self.replication.replid = replid;
        // Update hash ring with leader's replication ID and identifier
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    pub(crate) async fn send_cluster_heartbeat(&mut self) {
        self.remove_idle_peers().await;

        let hop_count = Self::hop_count(FANOUT, self.members.len());
        let hb =
            self.replication.default_heartbeat(hop_count).set_cluster_nodes(self.cluster_nodes());
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
            err!("The given peer is in the ban list {}", heartbeat.from);
            return;
        }
        self.apply_banlist(std::mem::take(&mut heartbeat.ban_list)).await;
        self.update_cluster_members(&heartbeat.from, &heartbeat.cluster_nodes).await;
        self.join_peer_network_if_absent::<TcpStream>(heartbeat.cluster_nodes).await;
        self.gossip(heartbeat.hop_count).await;
        self.maybe_update_hashring(heartbeat.hashring).await;
    }

    pub(crate) async fn leader_req_consensus(&mut self, req: ConsensusRequest) {
        if !self.replication.is_leader() {
            req.callback.send("Write given to follower".into());
            return;
        }

        if self.client_sessions.is_processed(&req.session_req) {
            // mapping between early returned values to client result
            let key = req.entry.all_keys().into_iter().map(String::from).collect();
            req.callback.send(ConsensusClientResponse::AlreadyProcessed { key });
            return;
        };

        if let Some(pending_mig) = self.migrations_in_progress.as_mut() {
            pending_mig.add_req(req);
            return;
        }

        match self.hash_ring.key_ownership(req.entry.all_keys().into_iter()) {
            | Ok(replids) if replids.all_belongs_to(&self.replication.replid) => {
                self.req_consensus(req).await;
            },
            | Ok(replids) => {
                // To notify client's of what keys have been moved.
                // ! Still, client won't know where the key has been moved. The assumption here is client SHOULD have correct hashring information.
                let moved_keys = replids.except(&self.replication.replid).join(" ");
                req.callback.send(format!("MOVED {moved_keys}").into());
            },
            | Err(err) => {
                err!("{}", err);
                req.callback.send(err.to_string().into());
            },
        }
    }

    async fn req_consensus(&mut self, req: ConsensusRequest) {
        // * Check if the request has already been processed
        if let Err(err) = self.replication.logger.write_single_entry(
            req.entry,
            self.replication.term,
            req.session_req.clone(),
        ) {
            req.callback.send(ConsensusClientResponse::Err(err.to_string()));
            return;
        };
        let last_log_index = self.logger().last_log_index;

        let repl_cnt = self.replicas().count();
        // * If there are no replicas, we can send the response immediately
        if repl_cnt == 0 {
            let entry = self.logger().read_at(last_log_index).unwrap();
            self.increase_con_idx();
            let res = self.commit_entry(entry.entry, last_log_index).await;
            req.callback.send(ConsensusClientResponse::Result(res));
            return;
        }
        self.consensus_tracker.insert(
            last_log_index,
            LogConsensusVoting::new(req.callback, repl_cnt, req.session_req),
        );
        self.send_rpc_to_replicas().await;
    }

    fn increase_con_idx(&mut self) {
        self.replication.logger.con_idx.fetch_add(1, Ordering::Relaxed);
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
        if self.find_replica_mut(&request_vote.candidate_id).is_none() {
            return;
        };
        REQUESTS_BLOCKED_BY_ELECTION.store(true, Ordering::Relaxed);

        let grant_vote = self.logger().last_log_index <= request_vote.last_log_index
            && self.replication.become_follower_if_term_higher_and_votable(
                &request_vote.candidate_id,
                request_vote.term,
            );

        info!(
            "Voting for {} with term {} and granted: {grant_vote}",
            request_vote.candidate_id, request_vote.term
        );

        // ! vote may have been made for requester, maybe not. Therefore, we have to set the term for self
        let vote = ElectionVote { term: self.replication.term, vote_granted: grant_vote };
        let Some(peer) = self.find_replica_mut(&request_vote.candidate_id) else {
            return;
        };

        let _ = peer.send(vote).await;
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, from,repl_res), fields(peer_id = %from))]
    pub(crate) async fn ack_replication(
        &mut self,
        from: &PeerIdentifier,
        repl_res: ReplicationAck,
    ) {
        let Some(peer) = self.members.get_mut(from) else {
            return;
        };

        peer.set_match_idx(repl_res.log_idx);

        if !repl_res.is_granted() {
            info!("vote cannot be granted {:?}", repl_res.rej_reason);
            self.handle_repl_rejection(repl_res.rej_reason).await;
            return;
        }

        self.track_replication_progress(from).await;
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, heartbeat), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn append_entries_rpc(&mut self, heartbeat: HeartBeat) {
        if self.check_term_outdated(&heartbeat).await {
            err!("Term Outdated received:{} self:{}", heartbeat.term, self.replication.term);
            return;
        };
        self.reset_election_timeout(heartbeat.term);

        self.replicate(heartbeat).await;

        // TODO Replace the following with watcher
        REQUESTS_BLOCKED_BY_ELECTION.store(false, Ordering::Relaxed);
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, election_vote))]
    pub(crate) async fn receive_election_vote(&mut self, election_vote: ElectionVote) {
        if !election_vote.vote_granted {
            return;
        }
        if !self.replication.election_state.can_transition_to_leader() {
            return;
        }

        info!("\x1b[32m{} elected as new leader\x1b[0m", self.replication.self_identifier());
        self.become_leader().await;

        // * Replica notification
        self.send_rpc_to_replicas().await;

        // * Cluster notification
        let msg = self.replication.default_heartbeat(0).set_hashring(self.hash_ring.clone());
        self.send_heartbeat(msg).await;
    }

    // * Forces the current node to become a replica of the given peer.
    pub(crate) async fn replicaof<C: TAsyncReadWrite>(
        &mut self,
        peer_addr: PeerIdentifier,
        callback: Callback<anyhow::Result<()>>,
    ) {
        self.cache_manager.drop_cache().await;
        self.replication.logger.reset();
        self.replication.logger.con_idx.store(0, Ordering::Release);
        self.set_repl_id(ReplicationId::Undecided);
        self.step_down().await;
        self.connect_to_server::<C>(peer_addr, Some(callback)).await;
    }

    // ! Callback chaining is required as otherwise it will block cluster actor from consuming more messages.
    // ! For example, trying to connect to a server requires handshakes and if it is not done in the background, actor will fail to receive the next message
    // ! Upon completion, we also conditionally activate cluster sync - which requires communication with actor itself.
    // ! And then finally, if cluster sync is activated wait until this node joins "cluster", not just a signgle server
    pub(crate) async fn cluster_meet<C: TAsyncReadWrite>(
        &mut self,
        peer_addr: PeerIdentifier,
        lazy_option: LazyOption,
        cl_cb: Callback<anyhow::Result<()>>,
    ) {
        if !self.replication.is_leader() || self.replication.self_identifier() == peer_addr {
            cl_cb.send(res_err!("wrong address or invalid state for cluster meet command"));
            return;
        }

        let (res_callback, conn_awaiter) = Callback::create();
        self.connect_to_server::<C>(peer_addr.clone(), Some(res_callback)).await;

        let (completion_sig, on_conn_completion) = Callback::create();
        tokio::spawn({
            let h = self.self_handler.clone();
            let lazy_option = lazy_option.clone();
            async move {
                let conn_res = conn_awaiter.recv().await;

                if lazy_option == LazyOption::Eager {
                    let (activation_sig, on_activation) = Callback::create();
                    let _ = h.send(ConnectionMessage::ActivateClusterSync(activation_sig)).await;
                    let _ = on_activation.recv().await;
                }
                completion_sig.send(conn_res);
            }
        });

        tokio::spawn(Self::register_delayed_schedule(
            self.self_handler.clone(),
            on_conn_completion,
            cl_cb,
            SchedulerMessage::RebalanceRequest { request_to: peer_addr, lazy_option },
        ));
    }

    async fn register_delayed_schedule<C>(
        cluster_sender: ClusterActorSender,
        completion_awaiter: CallbackAwaiter<anyhow::Result<C>>,
        on_completion: Callback<anyhow::Result<C>>,

        schedule_cmd: SchedulerMessage,
    ) where
        C: Send + Sync + 'static,
    {
        let result = completion_awaiter.recv().await;
        on_completion.send(result);

        let (callback, cluster_sync_awaiter) = Callback::create();
        let _ = cluster_sender.send(ConnectionMessage::RequestClusterSyncAwaiter(callback)).await;
        if let Some(sync_singal) = cluster_sync_awaiter.recv().await {
            let _ = sync_singal.recv().await;
        }

        // Send schedule command regardless of callback result
        if let Err(e) = cluster_sender.send(schedule_cmd).await {
            // Consider logging this error instead of silently ignoring
            error!("Failed to send schedule command: {}", e);
        }
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
            if peer.is_replica(&self.replication.replid) {
                warn!("Cannot rebalance to a replica: {}", request_to);
                return;
            }

            let peer = self.members.get_mut(&request_to).unwrap();
            let _ = peer.send(QueryIO::StartRebalance).await;
        }
    }

    #[instrument(level = tracing::Level::INFO, skip(self))]
    pub(crate) async fn start_rebalance(&mut self) {
        if !self.replication.is_leader() {
            err!("follower cannot start rebalance");
            return;
        }

        let Some(new_hashring) = self.hash_ring.set_partitions(self.shard_leaders()) else {
            warn!("No need for update on hashring");
            return;
        };

        warn!("Rebalancing started!");

        let hb = self
            .replication
            .default_heartbeat(Self::hop_count(FANOUT, self.members.len()))
            .set_hashring(new_hashring.clone());

        self.send_heartbeat(hb).await;
        self.maybe_update_hashring(Some(Box::new(new_hashring))).await;
    }

    fn hop_count(fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }

    fn replicas(&self) -> impl Iterator<Item = (&PeerIdentifier, u64)> {
        self.members.iter().filter_map(|(id, peer)| {
            (peer.is_replica(&self.replication.replid)).then_some((id, peer.curr_match_index()))
        })
    }

    pub(crate) fn get_sorted_roles(&mut self) -> Vec<(PeerIdentifier, ReplicationRole)> {
        let mut replica = self
            .members
            .iter()
            .filter(|(_, peer_state)| peer_state.is_replica(&self.replication.replid))
            .map(|(peer_id, peer_state)| (peer_id.clone(), peer_state.role().clone()))
            .chain(iter::once((self.replication.self_identifier(), self.replication.role.clone())))
            .collect::<Vec<_>>();
        replica.sort_by_key(|(_, role)| role.clone());
        replica
    }

    fn shard_leaders(&self) -> Vec<(ReplicationId, PeerIdentifier)> {
        let iter = self
            .members
            .iter()
            .filter(|(_, peer)| peer.role() == ReplicationRole::Leader)
            .map(|(id, peer)| (peer.replid().clone(), id.clone()));

        if self.replication.is_leader() {
            iter.chain(iter::once((
                self.replication.replid.clone(),
                self.replication.self_identifier(),
            )))
            .collect()
        } else {
            iter.collect()
        }
    }

    fn replicas_mut(&mut self) -> impl Iterator<Item = (&mut Peer, u64)> {
        self.members.values_mut().filter_map(|peer| {
            let log_index = peer.curr_match_index();
            (peer.is_replica(&self.replication.replid)).then_some((peer, log_index))
        })
    }

    fn find_replica_mut(&mut self, peer_id: &PeerIdentifier) -> Option<&mut Peer> {
        self.members.get_mut(peer_id).filter(|peer| peer.is_replica(&self.replication.replid))
    }

    fn peerid_by_replid(&self, target_repl_id: &ReplicationId) -> Option<&PeerIdentifier> {
        self.members
            .iter()
            .find(|(_, peer)| peer.replid() == target_repl_id)
            .map(|(peer_id, _)| peer_id)
    }

    async fn send_heartbeat(&mut self, heartbeat: HeartBeat) {
        self.members
            .values_mut()
            .map(|peer| peer.send(heartbeat.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    async fn snapshot_topology(&mut self) -> anyhow::Result<()> {
        let topology = self
            .cluster_nodes()
            .into_iter()
            .map(|cn| cn.format(&self.replication.self_identifier()))
            .collect::<Vec<_>>()
            .join("\r\n");
        self.topology_writer.seek(std::io::SeekFrom::Start(0))?;
        self.topology_writer.set_len(topology.len() as u64)?;
        self.topology_writer.write_all(topology.as_bytes())?;
        Ok(())
    }

    // ! BLOCK subsequent requests until rebalance is done
    fn block_write_reqs(&mut self) {
        if self.migrations_in_progress.is_none() {
            self.migrations_in_progress = Some(Default::default());
        }
    }

    // * Broadcasts the current topology to all connected clients
    fn broadcast_topology_change(&self) {
        self.node_change_broadcast.send(self.get_topology()).ok();
    }

    pub(crate) fn get_topology(&self) -> Topology {
        Topology::new(
            self.members
                .values()
                .clone()
                .map(|peer| NodeReplInfo::from_peer_state(peer.state()))
                .chain(iter::once(NodeReplInfo::from_replication_state(self.replication.info())))
                .collect(),
            self.hash_ring.clone(),
        )
    }

    async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        self.cluster_join_sync.known_peers.remove(peer_addr);

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
            .filter(|&(_, peer)| peer.phi.is_dead(now))
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

        let hb =
            self.replication.default_heartbeat(hop_count).set_cluster_nodes(self.cluster_nodes());
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

    async fn send_rpc_to_replicas(&mut self) {
        self.iter_follower_append_entries()
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
    fn iter_follower_append_entries(
        &mut self,
    ) -> Box<dyn Iterator<Item = (&mut Peer, HeartBeat)> + '_> {
        let lowest_watermark = self.take_low_watermark();
        let append_entries = self.replication.logger.list_append_log_entries(lowest_watermark);
        let default_heartbeat: HeartBeat = self.replication.default_heartbeat(0);

        // Handle empty entries case
        if append_entries.is_empty() {
            return Box::new(
                self.replicas_mut().map(move |(peer, _)| (peer, default_heartbeat.clone())),
            );
        }

        // If we have entries, find the entry before the first one to use as backup
        let backup_entry = self.replication.logger.read_at(append_entries[0].log_index - 1);

        let iterator = self.replicas_mut().map(move |(peer, p_log_idx)| {
            let missing_entries = append_entries
                .iter()
                .filter(|op| op.log_index > p_log_idx)
                .cloned()
                .collect::<Vec<_>>();

            let (prev_log_index, prev_log_term) = if missing_entries.len() == append_entries.len() {
                // * For Follower that require all entries, use backup entry to return prev_log_index and prev_term if exists
                backup_entry.as_ref().map(|entry| (entry.log_index, entry.term)).unwrap_or((0, 0))
            } else {
                // * Follower has some entries already, use the last one it has
                let last_log = &append_entries[append_entries.len() - missing_entries.len() - 1];
                (last_log.log_index, last_log.term)
            };

            let heart_beat = default_heartbeat
                .clone()
                .set_append_entries(missing_entries)
                .set_prev_log(prev_log_index, prev_log_term);
            (peer, heart_beat)
        });

        Box::new(iterator)
    }

    fn take_low_watermark(&self) -> Option<u64> {
        self.members
            .values()
            .filter(|peer| peer.is_replica(&self.replication.replid))
            .map(|peer| peer.curr_match_index())
            .min()
    }

    async fn track_replication_progress(&mut self, voter: &PeerIdentifier) {
        let Some(peer) = self.find_replica_mut(voter) else {
            return;
        };
        let log_index = peer.curr_match_index();

        let Some(mut voting) = self.consensus_tracker.remove(&log_index) else {
            return;
        };

        if voting.is_eligible_voter(voter) {
            info!("Received acks for log index num: {}", log_index);
            voting.increase_vote(voter.clone());
        }
        if voting.cnt < voting.get_required_votes() {
            self.consensus_tracker.insert(log_index, voting);
            return;
        }

        self.increase_con_idx();
        self.client_sessions.set_response(voting.session_req.take());

        let log_entry = self.logger().read_at(log_index).unwrap();
        let res = self.commit_entry(log_entry.entry, log_index).await;

        voting.callback.send(ConsensusClientResponse::Result(res));
    }

    async fn commit_entry(&mut self, entry: LogEntry, index: u64) -> anyhow::Result<QueryIO> {
        // TODO could be  if self.last_applied < index{ .. }
        let res = self.cache_manager.apply_entry(entry, index).await;
        self.replication.last_applied = index;

        res
    }

    // Follower notified the leader of its acknowledgment, then leader store match index for the given follower
    async fn send_replication_ack(&mut self, send_to: &PeerIdentifier, ack: ReplicationAck) {
        if let Some(leader) = self.members.get_mut(send_to) {
            if let Err(e) = leader.send(ack).await {
                // Log the error or handle it appropriately
                tracing::warn!("Failed to send replication ack to {}: {}", send_to, e);
            }
        } else {
            tracing::warn!("Attempted to send replication ack to unknown peer: {}", send_to);
        }
    }

    async fn replicate(&mut self, mut heartbeat: HeartBeat) {
        if self.replication.is_leader() {
            return;
        }

        // * write logs
        if let Err(rej_reason) = self.replicate_log_entries(&mut heartbeat).await {
            self.send_replication_ack(
                &heartbeat.from,
                ReplicationAck::reject(
                    self.logger().last_log_index,
                    rej_reason,
                    self.replication.term,
                ),
            )
            .await;
            return;
        };

        self.replicate_state(heartbeat).await;
    }

    async fn replicate_log_entries(&mut self, rpc: &mut HeartBeat) -> Result<(), RejectionReason> {
        if rpc.append_entries.is_empty() {
            return Ok(());
        }
        let mut entries = Vec::with_capacity(rpc.append_entries.len());
        let mut session_reqs = Vec::with_capacity(rpc.append_entries.len());

        for mut log in std::mem::take(&mut rpc.append_entries) {
            if log.log_index > self.logger().last_log_index {
                if let Some(session_req) = log.session_req.take() {
                    session_reqs.push(session_req);
                }
                entries.push(log);
            }
        }

        self.ensure_prev_consistency(rpc.prev_log_index, rpc.prev_log_term).await?;
        let log_idx = self.replication.logger.follower_write_entries(entries).map_err(|e| {
            err!("{}", e);
            RejectionReason::FailToWrite
        })?;

        self.send_replication_ack(&rpc.from, ReplicationAck::ack(log_idx, self.replication.term))
            .await;

        // * The following is to allow for failure of leader and follower elected as new leader.
        // Subscription request with the same session req should be treated as idempotent operation
        session_reqs.into_iter().for_each(|req| self.client_sessions.set_response(Some(req)));

        Ok(())
    }

    async fn ensure_prev_consistency(
        &mut self,

        prev_log_index: u64,
        prev_log_term: u64,
    ) -> Result<(), RejectionReason> {
        // Case: Empty log
        if self.replication.logger.is_empty() {
            if prev_log_index == 0 {
                return Ok(()); // First entry, no previous log to check
            }
            err!("Log is empty but leader expects an entry");
            return Err(RejectionReason::LogInconsistency); // Log empty but leader expects an entry
        }

        // * Raft followers should truncate their log starting at prev_log_index + 1 and then append the new entries
        // * Just returning an error is breaking consistency
        if let Some(prev_entry) = self.replication.logger.read_at(prev_log_index)
            && prev_entry.term != prev_log_term
        {
            // ! Term mismatch -> triggers log truncation
            err!("Term mismatch: {} != {}", prev_entry.term, prev_log_term);
            self.replication.logger.truncate_after(prev_log_index);

            return Err(RejectionReason::LogInconsistency);
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::INFO, skip(self))]
    pub(crate) async fn run_for_election(&mut self) {
        self.become_candidate();
        let request_vote = RequestVote::new(self.replication.info(), &self.replication.logger);

        self.replicas_mut()
            .map(|(peer, _)| peer.send(request_vote.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    fn reset_election_timeout(&mut self, new_term: u64) {
        self.heartbeat_scheduler.reset_election_timeout();
        self.replication.election_state = ElectionState::Follower { voted_for: None };
        self.replication.role = ReplicationRole::Follower;
        if new_term > self.replication.term {
            self.replication.term = new_term;
        }
    }

    async fn replicate_state(&mut self, leader_hb: HeartBeat) {
        let old_con_idx = self.replication.logger.con_idx.load(Ordering::Relaxed);
        let leader_commit_idx =
            leader_hb.leader_commit_idx.expect("Leader must send leader commit index");
        if leader_commit_idx > old_con_idx {
            let logs = self.replication.logger.range(old_con_idx, leader_commit_idx);

            for log in logs {
                self.increase_con_idx();

                if let Err(e) = self.commit_entry(log.entry, log.log_index).await {
                    // ! DON'T PANIC - post validation is where we just don't update state
                    // ! Failure of apply_log means post_validation on the operations that involves delta change such as incr/append fail.
                    // ! This is expected and you should let it update con_idx.
                    error!("failed to apply log: {e}, perhaps post validation failed?")
                }
            }
            self.cache_manager.pings().await;

            // * send the latest log index
            self.send_replication_ack(
                &leader_hb.from,
                ReplicationAck::ack(self.logger().last_log_index, self.replication.term),
            )
            .await;
        }
    }

    async fn check_term_outdated(&mut self, heartbeat: &HeartBeat) -> bool {
        if heartbeat.term < self.replication.term {
            self.send_replication_ack(
                &heartbeat.from,
                ReplicationAck::reject(
                    self.logger().last_log_index,
                    RejectionReason::ReceiverHasHigherTerm,
                    self.replication.term,
                ),
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
        self.replication.role = ReplicationRole::Leader;
        self.replication.election_state = ElectionState::Leader;
        self.heartbeat_scheduler.turn_leader_mode().await;
        self.hash_ring.update_repl_leader(
            self.replication.replid.clone(),
            self.replication.self_identifier(),
        );
        let _ =
            self.replication.logger.write_single_entry(LogEntry::NoOp, self.replication.term, None);

        // * force reconciliation
        let logs_to_reconcile =
            self.logger().range(self.replication.last_applied, self.logger().last_log_index);
        for op in logs_to_reconcile {
            self.increase_con_idx();
            if let Err(e) = self.commit_entry(op.entry, op.log_index).await {
                error!("failed to apply log: {e}, perhaps post validation failed?")
            }
        }
        REQUESTS_BLOCKED_BY_ELECTION.store(false, Ordering::Relaxed);
    }
    fn become_candidate(&mut self) {
        REQUESTS_BLOCKED_BY_ELECTION.store(true, Ordering::Relaxed);
        self.replication.term += 1;
        self.replication.election_state = ElectionState::Candidate {
            voting: Some(ElectionVoting::new(self.replicas().count() as u8)),
        };
    }

    async fn handle_repl_rejection(&mut self, repl_res: Option<RejectionReason>) {
        let Some(reason) = repl_res else {
            return;
        };

        info!("vote cannot be granted {:?}", reason);
        match reason {
            | RejectionReason::ReceiverHasHigherTerm => self.step_down().await,
            | RejectionReason::LogInconsistency => {
                info!("Log inconsistency, reverting match index");
            },
            | RejectionReason::FailToWrite => {
                info!("Follower failed to write log for technical reason, resend..");
            },
        }
    }

    async fn join_peer_network_if_absent<C: TAsyncReadWrite>(
        &mut self,
        cluster_nodes: Vec<PeerState>,
    ) {
        let self_id = self.replication.self_identifier();

        // Find the first suitable peer to connect to

        for node in cluster_nodes {
            let node_id = node.id();

            // Skip if it's ourselves or has higher ID (avoid connection collisions)
            if node_id == &self_id || node_id >= &self_id {
                continue;
            }
            self.cluster_join_sync.known_peers.entry(node_id.clone()).or_insert(false);

            // Skip if already connected or banned
            if self.members.contains_key(node_id) || self.replication.in_ban_list(node_id) {
                continue;
            }

            self.connect_to_server::<C>(node_id.clone(), None).await;
        }

        self.signal_if_peer_discovery_complete();
    }

    // * cluster message comes in and connection was tried. At this point, if some task is waiting for the notification, send it.
    // Currently, this flow is meant for:
    // - "cluster meet eager" where rebalancing should be promtly triggered if and only if cluster join is made.
    fn signal_if_peer_discovery_complete(&mut self) {
        if self.cluster_join_sync.known_peers.values().all(|is_known| *is_known)
            && let Some(notifier) = self.cluster_join_sync.notifier.take()
        {
            notifier.send(());
        }
    }

    // * If the hashring is valid, make a plan to migrate data for each paritition
    async fn maybe_update_hashring(&mut self, hashring: Option<Box<HashRing>>) {
        let Some(new_ring) = hashring else {
            return;
        };

        if *new_ring == self.hash_ring {
            return;
        }
        if new_ring.last_modified < self.hash_ring.last_modified {
            warn!("Received outdated hashring, ignoring");
            return;
        }

        // For replicas, just update the hash ring and wait for leader to coordinate migrations
        if !self.replication.is_leader() {
            self.hash_ring = *new_ring;
            info!("Replica updated hash ring");
            return;
        }

        // Leader-only migration coordination logic below
        // Keep the old ring to compare with new ring for migration planning
        let keys = self.cache_manager.route_keys(None).await;
        let chunks_map = self.hash_ring.create_migration_chunks(&new_ring, keys);

        if chunks_map.is_empty() {
            info!("No migration chunks to schedule");
            self.hash_ring = *new_ring;
            let _ = self.node_change_broadcast.send(self.get_topology());
            return;
        }

        info!("Scheduling {} migration chunks(s)", chunks_map.len());
        self.block_write_reqs();

        let batch_handles = FuturesUnordered::new();
        for (target_replid, mut migration_chunks) in chunks_map {
            while !migration_chunks.is_empty() {
                let mut num = 0;
                let mut batch_to_migrate = Vec::new();

                // Create a batch of tasks up to a certain size.
                while let Some(chunk) = migration_chunks.pop() {
                    num += chunk.keys_to_migrate.len();
                    batch_to_migrate.push(chunk);
                    if num > 100 {
                        break;
                    }
                }

                // Spawn each batch as a separate task for parallel execution
                batch_handles.push(tokio::spawn(Self::schedule_migration_in_batch(
                    PendingMigrationTask::new(target_replid.clone(), batch_to_migrate),
                    self.self_handler.clone(),
                )));
            }
        }

        // Process all batches
        tokio::spawn(async move {
            batch_handles
                .for_each(
                    |result: Result<Result<(), anyhow::Error>, tokio::task::JoinError>| async {
                        if let Err(e) = result {
                            error!("Migration batch panicked: {}", e);
                        }
                    },
                )
                .await;
        });
    }

    async fn schedule_migration_in_batch(
        batch: PendingMigrationTask,
        handler: ClusterActorSender,
    ) -> anyhow::Result<()> {
        let (tx, rx) = Callback::create();
        handler.send(SchedulerMessage::ScheduleMigrationBatch(batch, tx)).await?;
        rx.recv().await
    }

    pub(crate) async fn migrate_batch(
        &mut self,
        pending_task: PendingMigrationTask,
        callback: impl Into<Callback<anyhow::Result<()>>>,
    ) {
        let callback = callback.into();
        //  Find target peer based on replication ID
        let Some(peer_id) = self.peerid_by_replid(&pending_task.target_repl).cloned() else {
            callback.send(res_err!("Target peer not found for replication ID"));
            return;
        };

        // Retrieve key-value data from cache
        let keys = pending_task
            .chunks
            .iter()
            .flat_map(|task| task.keys_to_migrate.iter().cloned())
            .collect::<Vec<_>>();

        let entries = self
            .cache_manager
            .route_mget(keys.clone())
            .await
            .into_iter()
            .filter_map(|e| e.filter(|e| !e.value.is_null()))
            .collect::<Vec<_>>();

        // Get mutable reference to the target peer
        let Some(target_peer) = self.members.get_mut(&peer_id) else {
            callback.send(res_err!("Target peer {} disappeared during migration", peer_id));
            return;
        };

        if let Some(p) = self.migrations_in_progress.as_mut() {
            p.store_batch(pending_task.batch_id.clone(), QueuedKeysToMigrate { callback, keys })
        }

        let _ = target_peer.send(BatchEntries { batch_id: pending_task.batch_id, entries }).await;
    }

    pub(crate) async fn receive_batch(
        &mut self,
        migrate_batch: BatchEntries,
        from: &PeerIdentifier,
    ) {
        // If cache entries are empty, skip consensus and directly send success ack
        if migrate_batch.entries.is_empty() {
            let Some(peer) = self.members.get_mut(from) else {
                warn!("No Member Found");
                return;
            };
            let _ = peer.send(QueryIO::MigrationBatchAck(migrate_batch.batch_id)).await;
            return;
        }

        let (callback, rx) = Callback::create();

        self.req_consensus(ConsensusRequest {
            entry: LogEntry::MSet { entries: migrate_batch.entries.clone() },
            callback,
            session_req: None,
        })
        .await;

        // * If there are replicas, we need to wait for the consensus to be applied which should be done in the background
        tokio::spawn({
            let handler = self.self_handler.clone();
            let cache_manager = self.cache_manager.clone();
            let send_to = from.clone();
            async move {
                rx.recv().await;
                let _ = cache_manager.route_mset(migrate_batch.entries.clone()).await; // reflect state change
                let _ = handler
                    .send(SchedulerMessage::SendBatchAck {
                        batch_id: migrate_batch.batch_id,
                        to: send_to,
                    })
                    .await;
            }
        });
    }

    pub(crate) async fn handle_migration_ack(&mut self, batch_id: BatchId) {
        let Some(pending) = self.migrations_in_progress.as_mut() else {
            warn!("No Pending migration map available");
            return;
        };

        let Some(pending_migration_batch) = pending.pop_batch(&batch_id) else {
            err!("Batch ID {:?} not found in pending migrations", batch_id);
            return;
        };

        // make consensus request for delete
        let (callback, rx) = Callback::create();
        let w_req = ConsensusRequest {
            entry: LogEntry::Delete { keys: pending_migration_batch.keys.clone() },
            callback,
            session_req: None,
        };
        self.req_consensus(w_req).await;
        // ! background synchronization is required.
        tokio::spawn({
            let handler = self.self_handler.clone();
            let cache_manager = self.cache_manager.clone();

            async move {
                rx.recv().await;
                let _ = cache_manager.route_delete(pending_migration_batch.keys).await; // reflect state change
                let _ = handler.send(SchedulerMessage::TryUnblockWriteReqs).await;
                pending_migration_batch.callback.send(Ok(()));
            }
        });
    }

    // New hash ring stored at this point with the current shard leaders
    pub(crate) fn unblock_write_reqs_if_done(&mut self) {
        let migrations_done =
            self.migrations_in_progress.as_ref().is_none_or(|p| p.num_batches() == 0);

        if migrations_done {
            if let Some(new_ring) = self.hash_ring.set_partitions(self.shard_leaders()) {
                self.hash_ring = new_ring;
            }
            let _ = self.node_change_broadcast.send(self.get_topology());
            if let Some(pending_mig) = self.migrations_in_progress.take() {
                info!("All migrations complete, processing pending requests.");

                let mut pending_reqs = pending_mig.pending_requests();
                if pending_reqs.is_empty() {
                    return;
                }

                let handler = self.self_handler.clone();
                tokio::spawn(async move {
                    while let Some(req) = pending_reqs.pop_front() {
                        if let Err(err) = handler
                            .send(ClusterCommand::Client(ClientMessage::LeaderReqConsensus(req)))
                            .await
                        {
                            error!("{}", err)
                        }
                    }
                });
            }
        }
    }

    pub(crate) async fn send_batch_ack(&mut self, batch_id: BatchId, to: PeerIdentifier) {
        let Some(peer) = self.members.get_mut(&to) else {
            return;
        };
        let _ = peer.send(QueryIO::MigrationBatchAck(batch_id)).await;
    }

    async fn update_cluster_members(&mut self, from: &PeerIdentifier, cluster_nodes: &[PeerState]) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.record_heartbeat();
        }

        for node in cluster_nodes.iter() {
            if let Some(peer) = self.members.get_mut(node.id()) {
                peer.set_role(node.role.clone())
            }
        }
    }
}
