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
use crate::domains::cluster_actors::consensus::election::ElectionVoting;
use crate::domains::cluster_actors::hash_ring::BatchId;
use crate::domains::cluster_actors::hash_ring::MigrationBatch;
use crate::domains::cluster_actors::hash_ring::PendingMigrationBatch;
use crate::domains::cluster_actors::topology::{NodeReplInfo, Topology};
use crate::domains::operation_logs::WriteRequest;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::domains::peers::command::BannedPeer;
use crate::domains::peers::command::ElectionVote;
use crate::domains::peers::command::HeartBeat;
use crate::domains::peers::command::MigrateBatch;
use crate::domains::peers::command::MigrationBatchAck;
use crate::domains::peers::command::RejectionReason;
use crate::domains::peers::command::ReplicationAck;
use crate::domains::peers::command::RequestVote;
use crate::domains::peers::connections::inbound::stream::InboundStream;
use crate::domains::peers::connections::outbound::stream::OutboundStream;
use crate::domains::peers::identifier::TPeerAddress;
use crate::domains::peers::peer::PeerState;
use crate::err;
use crate::from_to;
use crate::res_err;
use crate::types::Callback;
use crate::types::ConnectionStream;
use client_sessions::ClientSessions;

use heartbeat_scheduler::HeartBeatScheduler;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs::File;
use std::io::Seek;
use std::io::Write;
use std::iter;
use std::sync::atomic::Ordering;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;
#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct ClusterActor<T> {
    pub(crate) members: BTreeMap<PeerIdentifier, Peer>,
    pub(crate) replication: ReplicationState,
    pub(crate) node_timeout: u128,
    pub(crate) consensus_tracker: LogConsensusTracker,
    pub(crate) receiver: tokio::sync::mpsc::Receiver<ClusterCommand>,
    pub(crate) self_handler: ClusterCommandHandler,
    pub(crate) heartbeat_scheduler: HeartBeatScheduler,
    pub(crate) topology_writer: std::fs::File,
    pub(crate) node_change_broadcast: tokio::sync::broadcast::Sender<Topology>,

    // * Pending requests are used to store requests that are received while the actor is in the process of election/cluster rebalancing.
    // * These requests will be processed once the actor is back to a stable state.
    pub(crate) client_sessions: ClientSessions,
    pub(crate) logger: ReplicatedLogs<T>,
    pub(crate) hash_ring: HashRing,
    pub(crate) pending_requests: Option<VecDeque<ConsensusRequest>>,
    pub(crate) pending_migrations: Option<HashMap<BatchId, PendingMigrationBatch>>,
    cluster_join_sync: ClusterJoinSync,
}

#[derive(Debug, Default)]
struct ClusterJoinSync {
    known_peers: HashMap<PeerIdentifier, bool>,
    notifier: Option<Callback<()>>,
    waiter: Option<oneshot::Receiver<()>>,
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
from_to!(tokio::sync::mpsc::Sender<ClusterCommand>, ClusterCommandHandler);

impl<T: TWriteAheadLog> ClusterActor<T> {
    pub(crate) fn run(
        node_timeout: u128,
        topology_writer: std::fs::File,
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

    fn new(
        node_timeout: u128,
        init_repl_state: ReplicationState,
        heartbeat_interval_in_mills: u64,
        topology_writer: File,
        log_writer: T,
    ) -> Self {
        let (self_handler, receiver) = tokio::sync::mpsc::channel(100);
        let heartbeat_scheduler = HeartBeatScheduler::run(
            ClusterCommandHandler(self_handler.clone()),
            init_repl_state.is_leader(),
            heartbeat_interval_in_mills,
        );

        let (tx, _) = tokio::sync::broadcast::channel::<Topology>(100);
        let hash_ring = HashRing::default().add_partitions(vec![(
            init_repl_state.replid.clone(),
            init_repl_state.self_identifier(),
        )]);

        Self {
            logger: ReplicatedLogs::new(
                log_writer,
                init_repl_state.hwm.load(Ordering::Acquire),
                init_repl_state.term,
            ),
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
            cluster_join_sync: ClusterJoinSync::default(),
            pending_requests: None,
            pending_migrations: None,
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
            let hb = self
                .replication
                .default_heartbeat(0, self.logger.last_log_index, self.logger.last_log_term)
                .set_hashring(self.hash_ring.clone());
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
        callback: Callback<Option<oneshot::Receiver<()>>>,
    ) {
        callback.send(self.cluster_join_sync.waiter.take());
    }

    // TODO enable DI
    #[instrument(skip(self, optional_callback))]
    pub(crate) async fn connect_to_server<C: TAsyncReadWrite>(
        &mut self,
        connect_to: PeerIdentifier,
        optional_callback: Option<Callback<anyhow::Result<()>>>,
    ) {
        if self.replication.self_identifier() == connect_to {
            optional_callback.map(|cb| {
                cb.send(res_err!("Cannot connect to self"));
            });
            return;
        }

        let Ok(cluster_bind_addr) = connect_to.cluster_bind_addr() else {
            optional_callback.map(|cb| {
                cb.send(res_err!("invalid cluster bind address for {}", connect_to));
            });
            return;
        };

        let Ok((r, w)) = C::connect(&cluster_bind_addr).await else {
            optional_callback.map(|cb| {
                cb.send(res_err!("Failed to connect to {}", connect_to));
            });
            return;
        };

        let outbound_stream = OutboundStream {
            r,
            w,
            my_repl_info: self.replication.clone(),
            connected_node_info: None,
        };

        tokio::spawn(outbound_stream.add_peer(
            self.replication.self_port,
            self.self_handler.clone(),
            optional_callback,
        ));
    }

    pub(crate) fn accept_inbound_stream(&mut self, peer_stream: ConnectionStream) {
        let stream = peer_stream.0;
        let peer_host = stream.peer_addr().expect("Failed to get peer address");
        let ip = peer_host.ip().to_string();

        let inbound_stream = {
            let self_repl_info = self.replication.clone();
            let (read, write) = stream.into_split();
            InboundStream {
                r: read.into(),
                w: write.into(),
                host_ip: ip,
                self_repl_info,
                connected_peer_info: Default::default(),
            }
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

    #[instrument(level = tracing::Level::DEBUG, skip(self, heartbeat,cache_manager), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn receive_cluster_heartbeat(
        &mut self,
        mut heartbeat: HeartBeat,
        cache_manager: &CacheManager,
    ) {
        if self.replication.in_ban_list(&heartbeat.from) {
            err!("The given peer is in the ban list {}", heartbeat.from);
            return;
        }
        self.apply_banlist(std::mem::take(&mut heartbeat.ban_list)).await;
        self.update_cluster_members(&heartbeat.from, heartbeat.hwm, &heartbeat.cluster_nodes).await;
        self.join_peer_network_if_absent::<TcpStream>(heartbeat.cluster_nodes).await;
        self.gossip(heartbeat.hop_count).await;
        self.maybe_update_hashring(heartbeat.hashring, cache_manager).await;
    }

    pub(crate) async fn leader_req_consensus(&mut self, req: ConsensusRequest) {
        if let Some(pending_requests) = self.pending_requests.as_mut() {
            pending_requests.push_back(req);
            return;
        }
        if self.client_sessions.is_processed(&req.session_req) {
            // mapping between early returned values to client result
            let key = req.request.all_keys().into_iter().map(String::from).collect();
            req.callback.send(ConsensusClientResponse::AlreadyProcessed {
                key,
                index: self.logger.last_log_index,
            });
            return;
        };

        match self.hash_ring.list_replids_for_keys(&req.request.all_keys()) {
            | Ok(replids) if replids.keys().all(|replid| *replid == self.replication.replid) => {
                self.req_consensus(req).await;
            },
            | Ok(replids) => {
                // To notify client's of what keys have been moved.
                // ! Still, client won't know where the key has been moved. The assumption here is client SHOULD have correct hashring information.
                let moved_keys = replids
                    .iter()
                    .filter_map(|(replid, keys)| {
                        if *replid != self.replication.replid { Some(keys.iter()) } else { None }
                    })
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>()
                    .join(" ");
                req.callback.send(format!("MOVED {moved_keys}").into());
            },
            | Err(err) => {
                err!("{}", err);
                req.callback.send(err.to_string().into());
            },
        }
    }

    async fn req_consensus(&mut self, req: ConsensusRequest) {
        if !self.replication.is_leader() {
            req.callback.send("Write given to follower".into());
            return;
        }

        // * Check if the request has already been processed
        if let Err(err) = self.logger.write_single_entry(
            &req.request,
            self.replication.term,
            req.session_req.clone(),
        ) {
            req.callback.send(ConsensusClientResponse::Err(err.to_string()));
            return;
        };

        let repl_cnt = self.replicas().count();
        if repl_cnt == 0 {
            // * If there are no replicas, we can send the response immediately
            self.replication.hwm.fetch_add(1, Ordering::Relaxed);
            req.callback.send(ConsensusClientResponse::LogIndex(self.logger.last_log_index));
            return;
        }
        self.consensus_tracker.add(self.logger.last_log_index, req, repl_cnt);
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
        if self.find_replica_mut(&request_vote.candidate_id).is_none() {
            return;
        };

        let grant_vote = self.logger.last_log_index <= request_vote.last_log_index
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
    pub(crate) async fn ack_replication(&mut self, from: PeerIdentifier, repl_res: ReplicationAck) {
        self.update_peer_index(&from, repl_res.log_idx);

        if !repl_res.is_granted() {
            info!("vote cannot be granted {:?}", repl_res.rej_reason);
            self.handle_repl_rejection(repl_res.rej_reason).await;
            return;
        }

        self.track_replication_progress(from);
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, cache_manager,heartbeat), fields(peer_id = %heartbeat.from))]
    pub(crate) async fn append_entries_rpc(
        &mut self,
        cache_manager: &CacheManager,
        heartbeat: HeartBeat,
    ) {
        if self.check_term_outdated(&heartbeat).await {
            err!("Term Outdated received:{} self:{}", heartbeat.term, self.replication.term);
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
        if !self.replication.election_state.can_transition_to_leader() {
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

        // * update hash ring with the new leader
        self.hash_ring.update_repl_leader(
            self.replication.replid.clone(),
            self.replication.self_identifier(),
        );
        let msg = msg.set_hashring(self.hash_ring.clone());
        self.send_heartbeat(msg).await;
    }

    // * Forces the current node to become a replica of the given peer.
    pub(crate) async fn replicaof<C: TAsyncReadWrite>(
        &mut self,
        peer_addr: PeerIdentifier,
        callback: Callback<anyhow::Result<()>>,
    ) {
        self.logger.reset();
        self.replication.hwm.store(0, Ordering::Release);
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
        self.connect_to_server::<C>(peer_addr.clone(), Some(res_callback.into())).await;

        let (completion_sig, on_conn_completion) = Callback::create();
        tokio::spawn({
            let h = self.self_handler.clone();
            let lazy_option = lazy_option.clone();
            async move {
                let conn_res =
                    conn_awaiter.await.unwrap_or_else(|_| Err(anyhow::anyhow!("Channel closed")));

                if lazy_option == LazyOption::Eager {
                    let (activation_sig, on_activation) = Callback::create();
                    let _ =
                        h.send(ConnectionMessage::ActivateClusterSync(activation_sig.into())).await;
                    let _ = on_activation.await;
                }
                let _ = completion_sig.send(conn_res);
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
        cluster_sender: ClusterCommandHandler,
        completion_awaiter: tokio::sync::oneshot::Receiver<anyhow::Result<C>>,
        on_completion: Callback<anyhow::Result<C>>,

        schedule_cmd: SchedulerMessage,
    ) where
        C: Send + Sync + 'static,
    {
        let result =
            completion_awaiter.await.unwrap_or_else(|_| Err(anyhow::anyhow!("Channel closed")));
        on_completion.send(result);

        let (callback, cluster_sync_awaiter) = Callback::create();
        let _ = cluster_sender.send(ConnectionMessage::RequestClusterSyncAwaiter(callback)).await;
        if let Some(sync_singal) = cluster_sync_awaiter.await.unwrap() {
            let _ = sync_singal.await;
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

    #[instrument(level = tracing::Level::INFO, skip(self,cache_manager))]
    pub(crate) async fn start_rebalance(&mut self, cache_manager: &CacheManager) {
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
            .default_heartbeat(
                Self::hop_count(FANOUT, self.members.len()),
                self.logger.last_log_index,
                self.logger.last_log_term,
            )
            .set_hashring(new_hashring.clone());

        self.send_heartbeat(hb).await;
        self.maybe_update_hashring(Some(Box::new(new_hashring)), cache_manager).await;
    }

    fn hop_count(fanout: usize, node_count: usize) -> u8 {
        if node_count <= fanout {
            return 0;
        }
        node_count.ilog(fanout) as u8
    }

    fn replicas(&self) -> impl Iterator<Item = (&PeerIdentifier, u64)> {
        self.members.iter().filter_map(|(id, peer)| {
            (peer.is_replica(&self.replication.replid)).then_some((id, peer.match_index()))
        })
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
            let match_index = peer.match_index();
            (peer.is_replica(&self.replication.replid)).then_some((peer, match_index))
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
        for peer in self.members.values_mut() {
            let _ = peer.send(heartbeat.clone()).await;
        }
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
        if self.pending_requests.is_none() {
            self.pending_requests = Some(VecDeque::new());
            self.pending_migrations = Some(HashMap::new());
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
                .chain(iter::once(NodeReplInfo::from_replication_state(&self.replication)))
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

    fn update_peer_index(&mut self, from: &PeerIdentifier, log_index: u64) {
        if let Some(peer) = self.members.get_mut(from) {
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

        let append_entries = self.logger.list_append_log_entries(lowest_watermark);

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
        let backup_entry = self.logger.read_at(append_entries[0].log_index - 1);

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
            .filter_map(|peer| {
                if peer.is_replica(&self.replication.replid) {
                    Some(peer.match_index())
                } else {
                    None
                }
            })
            .min()
    }

    fn track_replication_progress(&mut self, from: PeerIdentifier) {
        let Some(peer) = self.find_replica_mut(&from) else {
            return;
        };
        let peer_idx = peer.match_index();

        let Some(mut consensus) = self.consensus_tracker.remove(&peer_idx) else {
            return;
        };

        if consensus.votable(&from) {
            info!("Received acks for log index num: {}", peer_idx);
            consensus.increase_vote(from);
        }
        if consensus.cnt < consensus.get_required_votes() {
            self.consensus_tracker.insert(peer_idx, consensus);
            return;
        }

        // * Increase the high water mark
        self.replication.hwm.fetch_add(1, Ordering::Relaxed);

        self.client_sessions.set_response(consensus.session_req.take());
        consensus.callback.send(ConsensusClientResponse::LogIndex(peer_idx));
    }

    // Follower notified the leader of its acknowledgment, then leader store match index for the given follower
    async fn send_replication_ack(&mut self, send_to: &PeerIdentifier, ack: ReplicationAck) {
        let Some(leader) = self.members.get_mut(send_to) else {
            return;
        };
        let _ = leader.send(ack).await;
    }

    async fn replicate(&mut self, mut heartbeat: HeartBeat, cache_manager: &CacheManager) {
        if self.replication.is_leader() {
            return;
        }

        // * write logs
        if let Err(rej_reason) = self.replicate_log_entries(&mut heartbeat).await {
            self.send_replication_ack(
                &heartbeat.from,
                ReplicationAck::reject(self.logger.last_log_index, rej_reason, &self.replication),
            )
            .await;
            return;
        };

        self.replicate_state(heartbeat, cache_manager).await;
    }

    async fn replicate_log_entries(&mut self, rpc: &mut HeartBeat) -> Result<(), RejectionReason> {
        if rpc.append_entries.is_empty() {
            return Ok(());
        }
        let mut entries = Vec::with_capacity(rpc.append_entries.len());
        let mut session_reqs = Vec::with_capacity(rpc.append_entries.len());

        for mut log in std::mem::take(&mut rpc.append_entries) {
            if log.log_index > self.logger.last_log_index {
                if let Some(session_req) = log.session_req.take() {
                    session_reqs.push(session_req);
                }
                entries.push(log);
            }
        }

        self.ensure_prev_consistency(rpc.prev_log_index, rpc.prev_log_term).await?;
        let match_index = self.logger.follower_write_entries(entries).map_err(|e| {
            err!("{}", e);
            RejectionReason::FailToWrite
        })?;

        self.send_replication_ack(&rpc.from, ReplicationAck::ack(match_index, &self.replication))
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
        if self.logger.is_empty() {
            if prev_log_index == 0 {
                return Ok(()); // First entry, no previous log to check
            }
            err!("Log is empty but leader expects an entry");
            return Err(RejectionReason::LogInconsistency); // Log empty but leader expects an entry
        }

        // * Raft followers should truncate their log starting at prev_log_index + 1 and then append the new entries
        // * Just returning an error is breaking consistency
        if let Some(prev_entry) = self.logger.read_at(prev_log_index) {
            debug!("Previous log entry: {:?}", prev_entry);
            if prev_entry.term != prev_log_term {
                // ! Term mismatch -> triggers log truncation
                err!("Term mismatch: {} != {}", prev_entry.term, prev_log_term);
                self.logger.truncate_after(prev_log_index);

                return Err(RejectionReason::LogInconsistency);
            }
        }

        Ok(())
    }

    #[instrument(level = tracing::Level::INFO, skip(self))]
    pub(crate) async fn run_for_election(&mut self) {
        warn!("Running for election term {}", self.replication.term);

        self.become_candidate();
        let request_vote = RequestVote::new(&self.replication, &self.logger);

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
                let Some(log) = self.logger.read_at(log_index) else {
                    warn!("log has never been replicated!");
                    self.send_replication_ack(
                        &leader_hwm.from,
                        ReplicationAck::reject(
                            self.logger.last_log_index,
                            RejectionReason::LogInconsistency,
                            &self.replication,
                        ),
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
            self.replication.role = ReplicationRole::Follower;
        }
    }

    async fn check_term_outdated(&mut self, heartbeat: &HeartBeat) -> bool {
        if heartbeat.term < self.replication.term {
            self.send_replication_ack(
                &heartbeat.from,
                ReplicationAck::reject(
                    self.logger.last_log_index,
                    RejectionReason::ReceiverHasHigherTerm,
                    &self.replication,
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
        info!("\x1b[32mElection succeeded\x1b[0m");

        self.replication.role = ReplicationRole::Leader;
        self.replication.election_state = ElectionState::Leader;
        self.heartbeat_scheduler.turn_leader_mode().await;
    }
    fn become_candidate(&mut self) {
        let replica_count = self.replicas().count() as u8;
        self.replication.term += 1;
        self.replication.election_state =
            ElectionState::Candidate { voting: Some(ElectionVoting::new(replica_count)) };
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
    async fn maybe_update_hashring(
        &mut self,
        hashring: Option<Box<HashRing>>,
        cache_manager: &CacheManager,
    ) {
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
        let keys = cache_manager.route_keys(None).await;
        let migration_plans = self.hash_ring.create_migration_tasks(&new_ring, keys);

        if migration_plans.is_empty() {
            info!("No migration tasks to schedule");
            self.hash_ring = *new_ring;
            let _ = self.node_change_broadcast.send(self.get_topology());
            return;
        }

        info!("Leader scheduling {} migration plan(s)", migration_plans.len());
        self.block_write_reqs();

        let batch_handles = FuturesUnordered::new();
        for (target_replid, mut migration_tasks) in migration_plans {
            while !migration_tasks.is_empty() {
                let mut num = 0;
                let mut batch_to_migrate = Vec::new();

                // Create a batch of tasks up to a certain size.
                while let Some(task) = migration_tasks.pop() {
                    num += task.key_len();
                    batch_to_migrate.push(task);
                    if num > 100 {
                        break;
                    }
                }

                // Spawn each batch as a separate task for parallel execution
                batch_handles.push(tokio::spawn(Self::schedule_migration_in_batch(
                    MigrationBatch::new(target_replid.clone(), batch_to_migrate),
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
        batch: MigrationBatch,
        handler: ClusterCommandHandler,
    ) -> anyhow::Result<()> {
        let (tx, rx) = Callback::create();
        handler.send(SchedulerMessage::ScheduleMigrationBatch(batch, tx.into())).await?;
        rx.await?
    }

    pub(crate) async fn migrate_batch(
        &mut self,
        target: MigrationBatch,
        cache_manager: &CacheManager,
        callback: impl Into<Callback<anyhow::Result<()>>>,
    ) {
        let callback = callback.into();
        //  Find target peer based on replication ID
        let Some(peer_id) = self.peerid_by_replid(&target.target_repl).cloned() else {
            callback.send(res_err!("Target peer not found for replication ID"));
            return;
        };

        // Retrieve key-value data from cache
        let keys = target
            .tasks
            .iter()
            .flat_map(|task| task.keys_to_migrate.iter().cloned())
            .collect::<Vec<_>>();

        let cache_entries = cache_manager
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

        self.pending_migrations
            .as_mut()
            .map(|p| p.insert(target.id.clone(), PendingMigrationBatch::new(callback, keys)));

        let _ = target_peer.send(MigrateBatch { batch_id: target.id, cache_entries }).await;
    }

    pub(crate) async fn receive_batch(
        &mut self,
        migrate_batch: MigrateBatch,
        cache_manager: &CacheManager,
        from: PeerIdentifier,
    ) {
        // If cache entries are empty, skip consensus and directly send success ack
        if migrate_batch.cache_entries.is_empty() {
            let Some(peer) = self.members.get_mut(&from) else {
                warn!("No Member Found");
                return;
            };
            let _ = peer.send(MigrationBatchAck::with_success(migrate_batch.batch_id)).await;
            return;
        }

        let (tx, rx) = Callback::create();

        self.req_consensus(ConsensusRequest::new(
            WriteRequest::MSet { entries: migrate_batch.cache_entries.clone() },
            tx,
            None,
        ))
        .await;

        // * If there are replicas, we need to wait for the consensus to be applied which should be done in the background
        tokio::spawn({
            let handler = self.self_handler.clone();
            let cache_manager = cache_manager.clone();
            async move {
                if rx.await.is_ok() {
                    let _ = cache_manager.route_mset(migrate_batch.cache_entries.clone()).await; // reflect state change
                    let _ = handler
                        .send(SchedulerMessage::SendBatchAck {
                            batch_id: migrate_batch.batch_id,
                            to: from,
                        })
                        .await;
                    return;
                }
                error!(
                    "Failed to write some keys during migration for batch {}",
                    migrate_batch.batch_id.0
                );
            }
        });
    }

    pub(crate) async fn handle_migration_ack(
        &mut self,
        ack: MigrationBatchAck,
        cache_manager: &CacheManager,
    ) {
        let Some(pending) = self.pending_migrations.as_mut() else {
            warn!("No Pending migration map available");
            return;
        };

        let Some(pending_migration_batch) = pending.remove(&ack.batch_id) else {
            err!("Batch ID {:?} not found in pending migrations", ack.batch_id);
            return;
        };

        if !ack.success {
            pending_migration_batch
                .callback
                .send(res_err!("Failed to send migration completion signal for batch"));
            return;
        }

        // make consensus request for delete
        let (tx, rx) = Callback::create();
        let w_req = ConsensusRequest::new(
            WriteRequest::Delete { keys: pending_migration_batch.keys.clone() },
            tx,
            None,
        );
        self.req_consensus(w_req).await;
        // ! background synchronization is required.
        tokio::spawn({
            let handler = self.self_handler.clone();
            let cache_manager = cache_manager.clone();

            async move {
                if rx.await.is_ok() {
                    let _ = cache_manager.route_delete(pending_migration_batch.keys).await; // reflect state change
                    let _ = handler.send(SchedulerMessage::TryUnblockWriteReqs).await;
                    pending_migration_batch.callback.send(Ok(()));
                }
            }
        });
    }

    // New hash ring stored at this point with the current shard leaders
    pub(crate) fn unblock_write_reqs_if_done(&mut self) {
        let migrations_done = self.pending_migrations.as_ref().is_none_or(|p| p.is_empty());

        if migrations_done {
            if let Some(new_ring) = self.hash_ring.set_partitions(self.shard_leaders()) {
                self.hash_ring = new_ring;
            }
            let _ = self.node_change_broadcast.send(self.get_topology());
            if let Some(mut pending_reqs) = self.pending_requests.take() {
                info!("All migrations complete, processing pending requests.");
                self.pending_migrations = None;

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
        let _ = peer.send(MigrationBatchAck::with_success(batch_id)).await;
    }

    async fn update_cluster_members(
        &mut self,
        from: &PeerIdentifier,
        hwm: u64,
        cluster_nodes: &[PeerState],
    ) {
        self.update_peer_index(from, hwm);
        let now = Instant::now();
        for node in cluster_nodes.iter() {
            if let Some(peer) = self.members.get_mut(node.id()) {
                peer.last_seen = now;
                peer.set_role(node.role.clone())
            }
        }
    }
}
