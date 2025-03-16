use crate::domains::append_only_files::interfaces::TWriteAheadLog;
use crate::domains::append_only_files::logger::Logger;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::commands::ClusterCommand;

use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::cluster_actors::{ClusterActor, FANOUT};
use tokio::sync::mpsc::Sender;

impl ClusterActor {
    pub(crate) async fn handle(
        mut self,
        wal: impl TWriteAheadLog,
        cache_manager: CacheManager,
    ) -> anyhow::Result<Self> {
        let mut logger = Logger::new(wal);

        while let Some(command) = self.receiver.recv().await {
            match command {
                ClusterCommand::AddPeer(add_peer_cmd) => {
                    self.add_peer(add_peer_cmd).await;
                },
                ClusterCommand::GetPeers(callback) => {
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
                },
                ClusterCommand::ClusterNodes(callback) => {
                    let _ = callback.send(self.cluster_nodes());
                },
                ClusterCommand::SendHeartBeat => {
                    let hop_count = self.hop_count(FANOUT, self.members.len());
                    self.send_liveness_heartbeat(hop_count).await;

                    // ! remove idle peers based on ttl.
                    // ! The following may need to be moved else where to avoid blocking the main loop
                    self.remove_idle_peers().await;
                },
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                },
                ClusterCommand::SetReplicationInfo { leader_repl_id, hwm } => {
                    self.set_replication_info(leader_repl_id, hwm);
                },
                ClusterCommand::ReceiveHeartBeat(mut heartbeat) => {
                    if self.replication.in_ban_list(&heartbeat.heartbeat_from) {
                        continue;
                    }
                    self.gossip(heartbeat.hop_count).await;
                    self.update_on_hertbeat_message(&heartbeat);
                    self.apply_ban_list(std::mem::take(&mut heartbeat.ban_list)).await;

                    // check if the heartbeat is from a leader
                    if self.replication.is_from_leader(&heartbeat) {
                        // TODO hack
                        self.replication.leader_replid = heartbeat.leader_replid.clone();
                        self.boost_leadership(&heartbeat);
                        self.heartbeat_scheduler.reset_election_timeout();
                        self.replicate(&mut logger, heartbeat, &cache_manager).await;
                    }
                },
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                },
                ClusterCommand::LeaderReqConsensus { log, sender } => {
                    // Skip consensus for no replicas
                    let _ = self.req_consensus(&mut logger, log, sender).await;
                },
                ClusterCommand::LeaderReceiveAcks(offsets) => {
                    self.apply_acks(offsets);
                },
                ClusterCommand::SendCommitHeartBeat { log_idx: offset } => {
                    self.send_commit_heartbeat(offset).await;
                },

                ClusterCommand::SendLeaderHeartBeat => {
                    self.send_leader_heartbeat().await;
                },
                ClusterCommand::InstallLeaderState(logs) => {
                    if logger.overwrite(logs.clone()).await.is_err() {
                        continue;
                    }
                    self.install_leader_state(logs, &cache_manager).await;
                },
                ClusterCommand::FetchCurrentState(sender) => {
                    let logs = logger.range(0, self.replication.hwm);
                    let _ = sender.send(logs);
                },
                ClusterCommand::StartLeaderElection => {
                    self.run_for_election(logger.log_index, self.replication.term).await;
                },
                ClusterCommand::VoteElection(request_vote) => {
                    self.vote_election(request_vote, logger.log_index).await;
                },
                ClusterCommand::ApplyElectionVote(request_vote_reply) => {
                    self.tally_vote(request_vote_reply).await;
                },
            }
        }
        Ok(self)
    }

    pub fn run(
        node_timeout: u128,
        heartbeat_interval: u64,
        init_replication: ReplicationState,
        cache_manager: CacheManager,

        wal: impl TWriteAheadLog,
    ) -> Sender<ClusterCommand> {
        let cluster_actor = ClusterActor::new(node_timeout, init_replication, heartbeat_interval);
        let actor_handler = cluster_actor.self_handler.clone();
        tokio::spawn(cluster_actor.handle(wal, cache_manager));
        actor_handler
    }
}
