use std::sync::atomic::Ordering;

use crate::domains::append_only_files::interfaces::TWriteAheadLog;
use crate::domains::append_only_files::logger::ReplicatedLogs;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::commands::{ClusterCommand, ConsensusClientResponse};

use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::cluster_actors::session::ClientSessions;
use crate::domains::cluster_actors::{ClusterActor, FANOUT};
use tokio::sync::mpsc::Sender;

impl ClusterActor {
    pub(crate) async fn handle(
        mut self,
        wal: impl TWriteAheadLog,
        cache_manager: CacheManager,
        mut client_sessions: ClientSessions,
    ) -> anyhow::Result<Self> {
        let mut repl_logs = ReplicatedLogs::new(wal, 0, 0);

        while let Some(command) = self.receiver.recv().await {
            match command {
                ClusterCommand::AddPeer(add_peer_cmd, callback) => {
                    self.add_peer(add_peer_cmd).await;
                    self.snapshot_topology().await;
                    if let Some(callback) = callback {
                        let _ = callback.send(());
                    }
                },
                ClusterCommand::GetPeers(callback) => {
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
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
                    let hop_count = Self::hop_count(FANOUT, self.members.len());
                    self.send_cluster_heartbeat(hop_count, &repl_logs).await;

                    // ! remove idle peers based on ttl.
                    // ! The following may need to be moved else where to avoid blocking the main loop
                    self.remove_idle_peers().await;
                },

                ClusterCommand::ClusterHeartBeat(mut heartbeat) => {
                    if self.replication.in_ban_list(&heartbeat.from) {
                        continue;
                    }
                    self.gossip(heartbeat.hop_count, &repl_logs).await;
                    self.update_on_hertbeat_message(&heartbeat.from, heartbeat.hwm);
                    self.apply_ban_list(std::mem::take(&mut heartbeat.ban_list)).await;
                },
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                },
                ClusterCommand::LeaderReqConsensus { log, callback, session_req } => {
                    if client_sessions.is_processed(&session_req) {
                        // TODO is it okay to send current log index?
                        let _ = callback
                            .send(ConsensusClientResponse::LogIndex(Some(repl_logs.log_index)));
                        continue;
                    };
                    self.req_consensus(&mut repl_logs, log, callback, session_req).await;
                },

                // Follower receives heartbeat from leader
                ClusterCommand::AppendEntriesRPC(heartbeat) => {
                    if self.check_term_outdated(&heartbeat, &repl_logs).await {
                        continue;
                    };
                    self.reset_election_timeout(&heartbeat.from);
                    self.maybe_update_term(heartbeat.term, &mut repl_logs);
                    self.replicate(&mut repl_logs, heartbeat, &cache_manager).await;
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
                    self.send_leader_heartbeat(&repl_logs).await;
                },
                ClusterCommand::InstallLeaderState(logs) => {
                    if repl_logs.overwrite(logs.clone()).await.is_err() {
                        continue;
                    }
                    self.install_leader_state(logs, &cache_manager).await;
                },
                ClusterCommand::FetchCurrentState(sender) => {
                    let logs = repl_logs.range(0, self.replication.hwm.load(Ordering::Acquire));
                    let _ = sender.send(logs);
                },
                ClusterCommand::StartLeaderElection => {
                    self.run_for_election(repl_logs.log_index, self.replication.term).await;
                },
                ClusterCommand::VoteElection(request_vote) => {
                    self.vote_election(request_vote, repl_logs.log_index).await;
                },
                ClusterCommand::ApplyElectionVote(request_vote_reply) => {
                    if !request_vote_reply.vote_granted {
                        continue;
                    }
                    self.tally_vote(&repl_logs).await;
                },
                ClusterCommand::ReplicaOf(peer_addr, callback) => {
                    cache_manager.drop_cache().await;
                    self.replicaof(peer_addr).await;
                    let _ = callback.send(());
                },
            }
        }
        Ok(self)
    }

    pub(crate) fn run(
        node_timeout: u128,
        topology_path: String,
        heartbeat_interval: u64,
        init_replication: ReplicationState,
        cache_manager: CacheManager,
        wal: impl TWriteAheadLog,
    ) -> Sender<ClusterCommand> {
        let cluster_actor =
            ClusterActor::new(node_timeout, init_replication, heartbeat_interval, topology_path);
        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(wal, cache_manager, ClientSessions::default()));
        actor_handler
    }
}
