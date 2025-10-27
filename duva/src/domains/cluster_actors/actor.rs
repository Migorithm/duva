use super::ClusterCommand;
use super::ConsensusClientResponse;
use super::ConsensusRequest;
use super::LazyOption;
use super::hash_ring::HashRing;
pub mod client_sessions;
pub(crate) mod heartbeat_scheduler;
use super::*;
use crate::domains::QueryIO;
use crate::domains::TAsyncReadWrite;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::actor::heartbeat_scheduler::SchedulerTimeOutCommand;
use crate::domains::cluster_actors::queue::ClusterActorQueue;
use crate::domains::cluster_actors::queue::ClusterActorReceiver;
use crate::domains::cluster_actors::queue::ClusterActorSender;
use crate::domains::cluster_actors::topology::Topology;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::BatchEntries;
use crate::domains::peers::command::BatchId;
use crate::domains::replications::messages::ElectionVote;
use crate::domains::replications::messages::RejectionReason;
use crate::domains::replications::messages::ReplicationAck;
use crate::domains::replications::messages::RequestVote;

use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::PendingMigrationTask;
use crate::domains::peers::command::PendingRequests;
use crate::domains::peers::command::QueuedKeysToMigrate;
use crate::domains::peers::connections::connection_types::ReadConnected;
use crate::domains::peers::connections::connection_types::WriteConnected;
use crate::domains::peers::connections::inbound::stream::InboundStream;
use crate::domains::peers::connections::outbound::stream::OutboundStream;
use crate::domains::peers::identifier::TPeerAddress;

use crate::domains::replications::replication::time_in_secs;
use crate::domains::replications::*;
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
use tokio::time::Duration;

use tokio::net::TcpStream;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;
#[cfg(test)]
mod tests;

const FANOUT: usize = 2;

#[derive(Debug)]
pub struct ClusterActor<T> {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: Replication<T>,
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
    pending_reqs: Option<PendingRequests>,
    cluster_join_sync: ClusterJoinSync,
    banlist: Vec<BannedPeer>,
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
        replication: Replication<T>,
        cache_manager: CacheManager,
    ) -> ClusterActorSender {
        let cluster_actor =
            ClusterActor::new(replication, heartbeat_interval, topology_writer, cache_manager);
        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle());
        actor_handler
    }

    pub(crate) fn log_state(&self) -> &ReplicationState {
        self.replication.state()
    }

    fn new(
        init_repl_state: Replication<T>,
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
            init_repl_state.clone_state().replid.clone(),
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
            pending_reqs: None,
            cache_manager,
            banlist: Vec::new(),
        }
    }

    fn in_ban_list(&self, peer_identifier: &PeerIdentifier) -> bool {
        let current_time = time_in_secs().unwrap();
        self.banlist.iter().any(|x| x.p_id == *peer_identifier && current_time - x.ban_time < 60)
    }

    fn remove_from_banlist(&mut self, peer_id: &PeerIdentifier) {
        if let Some(pos) = self.banlist.iter().position(|x| x.p_id == *peer_id) {
            self.banlist.swap_remove(pos);
        };
    }

    #[instrument(level = tracing::Level::INFO, skip(self, peer, optional_callback),fields(peer_id = %peer.id()))]
    pub(crate) async fn add_peer(
        &mut self,
        peer: Peer,
        optional_callback: Option<Callback<anyhow::Result<()>>>,
    ) {
        let peer_id = peer.id().clone();

        self.remove_from_banlist(&peer_id);

        // If the map did have this key present, the value is updated, and the old
        // value is returned. The key is not updated,
        if let Some(existing_peer) = self.members.insert(peer_id.clone(), peer) {
            existing_peer.kill().await;
        }

        self.broadcast_topology_change();
        let _ = self.snapshot_topology().await;

        let peer = self.members.get_mut(&peer_id).unwrap();
        if peer.is_follower(self.replication.replid()) && self.replication.is_leader() {
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
            OutboundStream { r, w, self_state: self.replication.clone_state(), peer_state: None };

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
            self_state: self.replication.clone_state(),
            peer_state: Default::default(),
        };

        tokio::spawn(inbound_stream.add_peer(self.self_handler.clone()));
    }

    #[instrument(level = tracing::Level::INFO, skip(self,replid))]
    pub(crate) fn follower_setup(&mut self, replid: ReplicationId) {
        self.replication.set_replid(replid);
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    pub(crate) async fn send_cluster_heartbeat(&mut self) {
        self.remove_idle_peers().await;

        let hop_count = Self::hop_count(FANOUT, self.members.len());
        let hb = self
            .replication
            .default_heartbeat(hop_count)
            .set_cluster_nodes(self.cluster_nodes())
            .set_banlist(self.banlist.clone());
        self.send_heartbeat(hb).await;
    }

    pub(crate) async fn forget_peer(
        &mut self,
        peer_addr: PeerIdentifier,
    ) -> anyhow::Result<Option<()>> {
        let res = self.remove_peer(&peer_addr).await;
        self.banlist.push(BannedPeer { p_id: peer_addr, ban_time: time_in_secs()? });
        Ok(res)
    }

    // #[instrument(level = tracing::Level::DEBUG, skip(self, heartbeat), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn receive_cluster_heartbeat(&mut self, mut heartbeat: HeartBeat) {
        if self.in_ban_list(&heartbeat.from) {
            err!("The given peer is in the ban list {}", heartbeat.from);
            return;
        }
        self.apply_banlist(&mut heartbeat).await;
        self.update_cluster_members(&heartbeat.from, &heartbeat.cluster_nodes).await;
        self.join_peer_network_if_absent::<TcpStream>(heartbeat.cluster_nodes).await;
        self.gossip(heartbeat.hop_count).await;
        self.maybe_update_hashring(heartbeat.hashring).await;
    }

    async fn apply_banlist(&mut self, heartbeat: &mut HeartBeat) {
        if heartbeat.banlist.is_empty() {
            return;
        }

        for banned_peer in std::mem::take(&mut heartbeat.banlist) {
            if let Some(pos) = self.banlist.iter().position(|x| x.p_id == banned_peer.p_id) {
                if banned_peer.ban_time > self.banlist[pos].ban_time {
                    self.banlist[pos] = banned_peer;
                }
            } else {
                self.banlist.push(banned_peer);
            }
        }

        let current_time_in_sec = time_in_secs().unwrap();
        self.banlist.retain(|node| current_time_in_sec - node.ban_time < 60);

        // remove remainings after applying banlist
        let selfid = self.replication.self_identifier();
        for banned_peer_id in self.banlist.iter().map(|p| p.p_id.clone()).collect::<Vec<_>>() {
            warn!("applying ban list! {} removes {}...", selfid, banned_peer_id);
            self.remove_peer(&banned_peer_id).await;
        }
    }

    pub(crate) async fn leader_req_consensus(&mut self, req: ConsensusRequest) {
        if !self.replication.is_leader() {
            req.callback.send(ConsensusClientResponse::Err {
                reason: "Write given to follower".into(),
                request_id: req.session_req.unwrap().request_id,
            });
            return;
        }

        if self.client_sessions.is_processed(&req.session_req) {
            // mapping between early returned values to client result
            let key = req.entry.all_keys().into_iter().map(String::from).collect();
            req.callback.send(ConsensusClientResponse::AlreadyProcessed {
                key,
                // TODO : remove unwrap
                request_id: req.session_req.unwrap().request_id,
            });
            return;
        };

        if let Some(pending_mig) = self.pending_reqs.as_mut() {
            pending_mig.add_req(req);
            return;
        }

        match self.hash_ring.key_ownership(req.entry.all_keys().into_iter()) {
            Ok(replids) if replids.all_belongs_to(&self.log_state().replid) => {
                self.req_consensus(req, 10.into()).await;
            },
            Ok(replids) => {
                // To notify client's of what keys have been moved.
                // ! Still, client won't know where the key has been moved. The assumption here is client SHOULD have correct hashring information.
                let moved_keys = replids.except(&self.log_state().replid).join(" ");
                req.callback.send(ConsensusClientResponse::Err {
                    reason: format!("Moved {moved_keys}"),
                    request_id: req.session_req.unwrap().request_id,
                })
            },
            Err(err) => {
                err!("{}", err);
                req.callback.send(ConsensusClientResponse::Err {
                    reason: err.to_string(),
                    request_id: req.session_req.unwrap().request_id,
                });
            },
        }
    }

    async fn req_consensus(&mut self, req: ConsensusRequest, send_in_mills: Option<u64>) {
        let log_index = self.replication.write_single_entry(
            req.entry,
            self.log_state().term,
            req.session_req.clone(),
        );

        let repl_cnt = self.replicas().count();
        // * If there are no replicas, we can send the response immediately
        if repl_cnt == 0 {
            let entry = self.replication.read_at(log_index).unwrap();
            self.replication.increase_con_idx_by(1);
            let _ = self.replication.flush();
            let res = self.commit_entry(entry.entry, log_index).await;
            req.callback.send(ConsensusClientResponse::Result { res, log_index });
            return;
        }
        self.consensus_tracker
            .insert(log_index, LogConsensusVoting::new(req.callback, repl_cnt, req.session_req));

        if let Some(send_in_mills) = send_in_mills {
            tokio::spawn({
                let sender = self.heartbeat_scheduler.get_handler();
                async move {
                    tokio::time::sleep(Duration::from_millis(send_in_mills)).await;
                    sender.send(SchedulerTimeOutCommand::ResetTimer).await
                }
            });
        };
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    pub(crate) async fn send_rpc(&mut self) {
        let _ = self.replication.flush();
        if !self.replication.is_leader() {
            return;
        }

        if self.replicas().count() == 0 {
            return;
        }
        self.send_rpc_to_replicas().await;
    }

    pub(crate) fn cluster_nodes(&self) -> Vec<ReplicationState> {
        self.members
            .values()
            .map(|p| p.state().clone())
            .chain(std::iter::once(self.replication.clone_state()))
            .collect()
    }
    // #[instrument(level = tracing::Level::INFO, skip(self, request_vote))]
    pub(crate) async fn vote_election(&mut self, request_vote: RequestVote) {
        if self.find_replica_mut(&request_vote.candidate_id).is_none() {
            return;
        };
        self.heartbeat_scheduler.reset_timeout();

        let current_term = self.log_state().term;
        let mut grant_vote = false;

        if request_vote.term >= current_term {
            // If candidate's term is higher, update own term and step down
            if request_vote.term > current_term {
                self.replication.set_term(request_vote.term);
                self.step_down().await; // Ensure follower mode
            }
            grant_vote = self.replication.grant_vote(&request_vote);
        } else {
            // If candidate's term is less than current term, reject
            warn!(
                "Rejecting vote for {} (term {}). My term is higher ({}).",
                request_vote.candidate_id, request_vote.term, current_term
            );
        }

        let vote = ElectionVote { term: self.log_state().term, vote_granted: grant_vote };
        match self.find_replica_mut(&request_vote.candidate_id) {
            Some(peer) => {
                let _ = peer.send(vote).await;
            },
            None => {
                self.replication.revert_voting(current_term, &request_vote.candidate_id);
            },
        };
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, from,repl_res), fields(peer_id = %from))]
    pub(crate) async fn ack_replication(
        &mut self,
        from: &PeerIdentifier,
        repl_res: ReplicationAck,
    ) {
        if !self.replication.is_leader() {
            return;
        }
        let Some(peer) = self.members.get_mut(from) else {
            return;
        };

        peer.set_match_idx(repl_res.log_idx);

        if !repl_res.is_granted() {
            info!("vote cannot be granted {:?}", repl_res.rej_reason);
            self.handle_repl_rejection(repl_res.rej_reason).await;
            return;
        }

        self.advance_consensus_on_ack(from).await;
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, heartbeat), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn append_entries_rpc(&mut self, heartbeat: HeartBeat) {
        if self.replication.is_leader() {
            return;
        }
        if self.check_term_outdated(&heartbeat).await {
            err!("Term Outdated received:{} self:{}", heartbeat.term, self.log_state().term);
            return;
        };
        self.reset_election_timeout(heartbeat.term);

        self.replicate(heartbeat).await;
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, election_vote), fields(from = %from))]
    pub(crate) async fn receive_election_vote(
        &mut self,
        from: &PeerIdentifier,
        election_vote: ElectionVote,
    ) {
        // If we receive a vote from a future term, we should step down.
        if election_vote.term > self.log_state().term {
            warn!("Received a vote from a future term {}, stepping down.", election_vote.term);
            self.replication.set_term(election_vote.term);
            self.step_down().await;
            return;
        }

        // A candidate should only process votes from its current term.
        if election_vote.term < self.log_state().term {
            warn!(
                "Received a stale vote with term {}, but current term is {}. Ignoring.",
                election_vote.term,
                self.log_state().term
            );
            return;
        }
        if !election_vote.vote_granted {
            return;
        }

        if self.replication.is_leader() {
            return;
        }

        // If we are not a candidate, ignore the vote.
        if !self.replication.is_candidate() {
            return;
        }

        // Record the vote
        if !self.replication.record_vote(from.clone()) {
            warn!("Received a duplicate vote from {}", from);
            return; // Already voted
        }

        // Check for majority
        if self.replication.has_majority_vote() {
            info!("\x1b[32m{} elected as new leader\x1b[0m", self.replication.self_identifier());
            self.become_leader().await;

            // * Replica notification
            self.send_rpc_to_replicas().await;

            // * Cluster notification
            let msg = self.replication.default_heartbeat(0).set_hashring(self.hash_ring.clone());
            self.send_heartbeat(msg).await;
        }
    }

    // * Forces the current node to become a replica of the given peer.
    pub(crate) async fn replicaof<C: TAsyncReadWrite>(
        &mut self,
        peer_addr: PeerIdentifier,
        callback: Callback<anyhow::Result<()>>,
    ) {
        self.cache_manager.drop_cache().await;
        self.replication.reset_log();

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
            if peer.is_replica(&self.log_state().replid) {
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
            (peer.is_replica(&self.log_state().replid)).then_some((id, peer.curr_match_index()))
        })
    }

    pub(crate) fn get_sorted_roles(&mut self) -> Vec<(PeerIdentifier, ReplicationRole)> {
        let mut replica = self
            .members
            .iter()
            .filter(|(_, peer_state)| peer_state.is_replica(&self.log_state().replid))
            .map(|(peer_id, peer_state)| (peer_id.clone(), peer_state.role().clone()))
            .chain(iter::once((self.replication.self_identifier(), self.log_state().role.clone())))
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
                self.log_state().replid.clone(),
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
            (peer.is_replica(self.replication.replid())).then_some((peer, log_index))
        })
    }

    fn find_replica_mut(&mut self, peer_id: &PeerIdentifier) -> Option<&mut Peer> {
        self.members.get_mut(peer_id).filter(|peer| peer.is_replica(self.replication.replid()))
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

    pub(crate) fn register_awaiter_if_pending(&mut self, callback: Callback<()>) {
        if let Some(pending_req) = self.pending_reqs.as_mut() {
            pending_req.callbacks.push(callback);
        } else {
            callback.send(())
        };
    }

    // ! BLOCK subsequent requests until rebalance is done
    fn block_write_reqs(&mut self) {
        if self.pending_reqs.is_none() {
            self.pending_reqs = Some(PendingRequests::default());
        }
    }

    // * Broadcasts the current topology to all connected clients
    fn broadcast_topology_change(&self) {
        self.node_change_broadcast.send(self.get_topology()).ok();
    }

    pub(crate) fn get_topology(&self) -> Topology {
        Topology {
            repl_states: self
                .members
                .values()
                .clone()
                .map(|peer| peer.state().clone())
                .chain(iter::once(self.replication.clone_state()))
                .collect(),
            hash_ring: self.hash_ring.clone(),
        }
    }

    pub(crate) fn get_leader_id(&self) -> Option<PeerIdentifier> {
        if self.replication.is_leader() {
            return Some(self.replication.self_identifier());
        }
        self.members
            .iter()
            .filter(|(_, peer)| *peer.replid() == self.log_state().replid)
            .find(|(_, peer)| peer.role() == ReplicationRole::Leader)
            .map(|(peer_id, _)| peer_id.clone())
    }

    async fn remove_peer(&mut self, peer_addr: &PeerIdentifier) -> Option<()> {
        self.cluster_join_sync.known_peers.remove(peer_addr);
        if let Some(peer) = self.members.remove(peer_addr) {
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
            warn!(
                "idle peer detected! {} removes {}...",
                self.replication.self_identifier(),
                peer_id
            );
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
            .default_heartbeat(hop_count)
            .set_cluster_nodes(self.cluster_nodes())
            .set_banlist(self.banlist.clone());

        self.send_heartbeat(hb).await;
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
        let append_entries = self.replication.list_append_log_entries(lowest_watermark);
        let default_heartbeat: HeartBeat = self.replication.default_heartbeat(0);

        // Handle empty entries case
        if append_entries.is_empty() {
            return Box::new(
                self.replicas_mut().map(move |(peer, _)| (peer, default_heartbeat.clone())),
            );
        }

        // If we have entries, find the entry before the first one to use as backup
        let backup_entry = self.replication.read_at(append_entries[0].log_index - 1);

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
            .filter_map(|peer| {
                if peer.is_replica(&self.log_state().replid) {
                    Some(peer.curr_match_index())
                } else {
                    None
                }
            })
            .min()
    }

    async fn advance_consensus_on_ack(&mut self, voter: &PeerIdentifier) {
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

        self.replication.increase_con_idx_by(1);
        self.client_sessions.set_response(voting.session_req.take());
        let log_entry = self.replication.read_at(log_index).unwrap();
        let res = self.commit_entry(log_entry.entry, log_index).await;
        let _ = self.replication.flush();

        voting.callback.send(ConsensusClientResponse::Result { res, log_index });
    }

    async fn commit_entry(&mut self, entry: LogEntry, index: u64) -> anyhow::Result<QueryIO> {
        // TODO could be  if self.last_applied < index{ .. }
        let res = self.cache_manager.apply_entry(entry, index).await;
        *self.replication.last_applied_mut() = index;

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
        if heartbeat.leader_commit_idx.is_none() {
            err!("It must have leader commit index!");
            return;
        }

        // * write logs
        if let Err(rej_reason) = self.replicate_log_entries(&mut heartbeat).await {
            self.send_replication_ack(
                &heartbeat.from,
                ReplicationAck::reject(
                    self.log_state().last_log_index,
                    rej_reason,
                    self.log_state().term,
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

        let mut session_reqs = Vec::with_capacity(rpc.append_entries.len());
        let ack = self.replication.replicate_log_entries(
            std::mem::take(&mut rpc.append_entries),
            rpc.prev_log_index,
            rpc.prev_log_term,
            &mut session_reqs,
        )?;

        self.send_replication_ack(&rpc.from, ack).await;

        // * The following is to allow for failure of leader and follower elected as new leader.
        // request with the same session req should be treated as idempotent operation
        for sr in session_reqs {
            self.client_sessions.set_response(Some(sr))
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::INFO, skip(self))]
    pub(crate) async fn run_for_election(&mut self) {
        if self.replication.is_leader() {
            return;
        }

        self.become_candidate();
        let request_vote = self.replication.request_vote();

        self.replicas_mut()
            .map(|(peer, _)| peer.send(request_vote.clone()))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    fn reset_election_timeout(&mut self, term: u64) {
        self.heartbeat_scheduler.reset_timeout();
        self.replication.on_election_timeout(term);
    }

    async fn replicate_state(&mut self, leader_hb: HeartBeat) -> Option<ReplicationAck> {
        let old_con_idx = self.replication.curr_con_idx();
        let leader_commit_idx =
            leader_hb.leader_commit_idx.expect("Leader must send leader commit index");
        if leader_commit_idx > old_con_idx {
            let logs = self.replication.range(old_con_idx, leader_commit_idx);
            self.replication.increase_con_idx_by(logs.len() as u64);

            for log in logs {
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
                ReplicationAck::ack(self.log_state().last_log_index, self.log_state().term),
            )
            .await;
        }

        None
    }

    async fn check_term_outdated(&mut self, heartbeat: &HeartBeat) -> bool {
        let log_state = self.log_state();

        if heartbeat.term < log_state.term {
            self.send_replication_ack(
                &heartbeat.from,
                ReplicationAck::reject(
                    log_state.last_log_index,
                    RejectionReason::ReceiverHasHigherTerm,
                    log_state.term,
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
        self.replication.reset_election_votes();
        self.heartbeat_scheduler.turn_follower_mode().await;

        self.unblock_pending_requests();
    }

    async fn become_leader(&mut self) {
        self.replication.set_role(ReplicationRole::Leader);
        self.replication.clear_votes();
        self.heartbeat_scheduler.turn_leader_mode().await;
        self.hash_ring.update_repl_leader(
            self.log_state().replid.clone(),
            self.replication.self_identifier(),
        );
        let last_index =
            self.replication.write_single_entry(LogEntry::NoOp, self.log_state().term, None);
        let _ = self.replication.flush();

        // * force reconciliation
        let logs_to_reconcile = self.replication.range(self.replication.last_applied(), last_index);

        self.replication.increase_con_idx_by(logs_to_reconcile.len() as u64);
        for op in logs_to_reconcile {
            if let Err(e) = self.commit_entry(op.entry, op.log_index).await {
                error!("failed to apply log: {e}, perhaps post validation failed?")
            }
        }

        self.unblock_pending_requests();
    }

    fn become_candidate(&mut self) {
        self.block_write_reqs();
        self.replication.set_term(self.log_state().term + 1);
        self.replication.initiate_vote(self.replicas().count());
    }

    async fn handle_repl_rejection(&mut self, repl_res: Option<RejectionReason>) {
        let Some(reason) = repl_res else {
            return;
        };

        info!("vote cannot be granted {:?}", reason);
        match reason {
            RejectionReason::ReceiverHasHigherTerm => self.step_down().await,
            RejectionReason::LogInconsistency => {
                info!("Log inconsistency, reverting match index");
            },
            RejectionReason::FailToWrite => {
                info!("Follower failed to write log for technical reason, resend..");
            },
        }
    }

    async fn join_peer_network_if_absent<C: TAsyncReadWrite>(
        &mut self,
        cluster_nodes: Vec<ReplicationState>,
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
            if self.members.contains_key(node_id) || self.in_ban_list(node_id) {
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
            .flat_map(|task| task.keys_to_migrate.iter())
            .cloned()
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

        // Send batch first, then store in pending ONLY IF successful
        if let Err(e) = target_peer
            .send(BatchEntries { batch_id: pending_task.batch_id.clone(), entries })
            .await
        {
            callback.send(res_err!("Failed to send migration batch: {}", e));
            return;
        }

        if let Some(p) = self.pending_reqs.as_mut() {
            p.store_batch(pending_task.batch_id, QueuedKeysToMigrate { callback, keys })
        }
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
        let req = ConsensusRequest {
            entry: LogEntry::MSet { entries: migrate_batch.entries.clone() },
            callback,
            session_req: None,
        };
        self.make_consensus_in_batch(req).await;

        // * If there are replicas, we need to wait for the consensus to be applied which should be done in the background
        tokio::spawn({
            let handler = self.self_handler.clone();
            let cache_manager = self.cache_manager.clone();
            let send_to = from.clone();
            async move {
                rx.recv().await;
                let _ = cache_manager.route_mset(migrate_batch.entries).await; // reflect state change
                let _ = handler
                    .send(SchedulerMessage::SendBatchAck {
                        batch_id: migrate_batch.batch_id,
                        to: send_to,
                    })
                    .await;
            }
        });
    }

    async fn make_consensus_in_batch(&mut self, req: ConsensusRequest) {
        self.req_consensus(req, None).await;
        let _ = self.replication.flush();
        self.send_rpc().await;
    }

    pub(crate) async fn handle_migration_ack(&mut self, batch_id: BatchId) {
        let Some(pending) = self.pending_reqs.as_mut() else {
            warn!("No Pending migration map available");
            return;
        };

        let Some(pending_migration_batch) = pending.pop_batch(&batch_id) else {
            err!("Batch ID {:?} not found in pending migrations", batch_id);
            return;
        };

        // make consensus request for delete
        let (callback, rx) = Callback::create();
        let req = ConsensusRequest {
            entry: LogEntry::Delete { keys: pending_migration_batch.keys.clone() },
            callback,
            session_req: None,
        };
        self.make_consensus_in_batch(req).await;

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
    pub(crate) fn unblock_write_on_migration_done(&mut self) {
        let migrations_done = self.pending_reqs.as_ref().is_none_or(|p| p.num_batches() == 0);

        if migrations_done {
            if let Some(new_ring) = self.hash_ring.set_partitions(self.shard_leaders()) {
                self.hash_ring = new_ring;
            }
            let _ = self.node_change_broadcast.send(self.get_topology());
            self.unblock_pending_requests();
        }
    }

    fn unblock_pending_requests(&mut self) {
        if let Some(mut pending_mig) = self.pending_reqs.take() {
            info!("All migrations complete, processing pending requests.");
            let handler = self.self_handler.clone();

            tokio::spawn(async move {
                let mut pending_reqs = pending_mig.extract_requests();

                while let Some(req) = pending_reqs.pop_front() {
                    if let Err(err) = handler
                        .send(ClusterCommand::Client(ClientMessage::LeaderReqConsensus(req)))
                        .await
                    {
                        error!("{}", err)
                    }
                }
                pending_mig.callbacks.into_iter().for_each(|c| c.send(()));
            });
        }
    }

    pub(crate) async fn send_batch_ack(&mut self, batch_id: BatchId, to: PeerIdentifier) {
        let Some(peer) = self.members.get_mut(&to) else {
            return;
        };
        let _ = peer.send(QueryIO::MigrationBatchAck(batch_id)).await;
    }

    async fn update_cluster_members(
        &mut self,
        from: &PeerIdentifier,
        cluster_nodes: &[ReplicationState],
    ) {
        if let Some(peer) = self.members.get_mut(from) {
            peer.record_heartbeat();
        }

        for node in cluster_nodes.iter() {
            if let Some(peer) = self.members.get_mut(node.id()) {
                peer.set_role(node.role.clone())
            }
        }
    }

    pub(crate) async fn process_graceful_shutdown(&mut self) {
        self.members
            .iter_mut()
            .map(|(_, p)| p.send(QueryIO::CloseConnection))
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;
    }

    pub(crate) async fn close_connection(&mut self, from: &PeerIdentifier) {
        warn!("{from} disconnected!");
        self.remove_peer(from).await;
    }
}
