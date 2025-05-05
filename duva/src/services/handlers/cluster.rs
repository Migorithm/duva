use std::collections::VecDeque;

use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::commands::{
    ClusterCommand, ConsensusClientResponse, ConsensusRequest,
};
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::cluster_actors::session::ClientSessions;
use crate::domains::cluster_actors::{ClusterActor, FANOUT};
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::logger::ReplicatedLogs;
use tokio::sync::mpsc::Sender;

impl ClusterActor {
    pub(crate) async fn handle(
        mut self,
        wal: impl TWriteAheadLog,
        cache_manager: CacheManager,
        mut client_sessions: ClientSessions,
    ) -> anyhow::Result<Self> {
        let mut logger = ReplicatedLogs::new(wal, 0, 0);

        while let Some(command) = self.receiver.recv().await {
            match command {
                ClusterCommand::DiscoverCluster { connect_to, callback } => {
                    if self.discover_cluster(connect_to).await.is_ok() {
                        let _ = self.snapshot_topology().await;
                    };
                    let _ = callback.send(());
                },

                ClusterCommand::AcceptPeer { stream } => {
                    if let Ok(()) = self.accept_inbound_stream(stream, &logger).await {
                        let _ = self.snapshot_topology().await;
                    };
                },

                ClusterCommand::GetPeers(callback) => {
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>());
                },
                ClusterCommand::ClusterNodes(callback) => {
                    let _ = callback.send(self.cluster_nodes());
                },
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                },
                ClusterCommand::SetReplicationInfo { replid: leader_repl_id, hwm } => {
                    self.set_replication_info(leader_repl_id, hwm);
                },
                ClusterCommand::SendClusterHeatBeat => {
                    // ! remove idle peers based on ttl.
                    // ! The following may need to be moved else where to avoid blocking the main loop
                    self.remove_idle_peers().await;
                    let hop_count = Self::hop_count(FANOUT, self.members.len());
                    self.send_cluster_heartbeat(hop_count, &logger).await;
                },
                ClusterCommand::ClusterHeartBeat(mut heartbeat) => {
                    if self.replication.in_ban_list(&heartbeat.from) {
                        continue;
                    }
                    self.apply_ban_list(std::mem::take(&mut heartbeat.ban_list)).await;
                    self.join_peer_network_if_absent(heartbeat.cluster_nodes).await;
                    self.gossip(heartbeat.hop_count, &logger).await;
                    self.update_on_hertbeat_message(&heartbeat.from, heartbeat.hwm);
                },
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                },
                ClusterCommand::LeaderReqConsensus(req) => {
                    if let Some(pending_requests) = self.pending_requests.as_mut() {
                        pending_requests.push_back(req);
                        continue;
                    }

                    if client_sessions.is_processed(&req.session_req) {
                        // TODO mapping between early returned values to client result
                        let _ = req.callback.send(Ok(ConsensusClientResponse::AlreadyProcessed {
                            key: req.request.key(),
                            index: logger.last_log_index,
                        }));
                        continue;
                    };

                    self.req_consensus(&mut logger, req).await;
                },
                ClusterCommand::AppendEntriesRPC(heartbeat) => {
                    if self.check_term_outdated(&heartbeat, &logger).await {
                        continue;
                    };

                    self.reset_election_timeout(&heartbeat.from);
                    self.maybe_update_term(heartbeat.term);
                    self.replicate(&mut logger, heartbeat, &cache_manager).await;
                },
                ClusterCommand::ReplicationResponse(repl_res) => {
                    if !repl_res.is_granted() {
                        self.handle_repl_rejection(repl_res).await;
                        continue;
                    }
                    self.update_on_hertbeat_message(&repl_res.from, repl_res.log_idx);
                    self.track_replication_progress(repl_res, &mut client_sessions);
                },
                ClusterCommand::SendCommitHeartBeat { log_idx: offset } => {
                    self.send_commit_heartbeat(offset).await;
                },
                ClusterCommand::SendAppendEntriesRPC => {
                    self.send_leader_heartbeat(&logger).await;
                },
                ClusterCommand::InstallLeaderState(logs) => {
                    if logger.follower_full_sync(logs.clone()).await.is_err() {
                        continue;
                    }
                    self.install_leader_state(logs, &cache_manager).await;
                },
                ClusterCommand::StartLeaderElection => {
                    self.run_for_election(&mut logger).await;
                },
                ClusterCommand::VoteElection(request_vote) => {
                    self.vote_election(request_vote, logger.last_log_index).await;
                },
                ClusterCommand::ApplyElectionVote(request_vote_reply) => {
                    if !request_vote_reply.vote_granted {
                        continue;
                    }
                    self.tally_vote(&logger).await;
                },
                ClusterCommand::ReplicaOf(peer_addr, callback) => {
                    cache_manager.drop_cache().await;
                    self.replicaof(peer_addr).await;
                    let _ = callback.send(());
                },
                ClusterCommand::GetRole(sender) => {
                    let _ = sender.send(self.replication.role.clone());
                },
                ClusterCommand::SubscribeToTopologyChange(sender) => {
                    let _ = sender.send(self.node_change_broadcast.subscribe());
                },
            }
        }
        Ok(self)
    }

    pub(crate) fn run(
        node_timeout: u128,
        topology_writer: tokio::fs::File,
        heartbeat_interval: u64,
        init_replication: ReplicationState,
        cache_manager: CacheManager,
        wal: impl TWriteAheadLog,
    ) -> Sender<ClusterCommand> {
        let cluster_actor =
            ClusterActor::new(node_timeout, init_replication, heartbeat_interval, topology_writer);

        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(wal, cache_manager, ClientSessions::default()));
        actor_handler
    }
}
