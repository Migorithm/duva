use crate::domains::append_only_files::interfaces::TAof;
use crate::domains::append_only_files::logger::Logger;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::consensus::ConsensusTracker;
use crate::domains::cluster_actors::{ClusterActor, FANOUT};
use tokio::sync::mpsc::Receiver;

impl ClusterActor {
    pub(crate) async fn handle(
        mut self,
        mut cluster_message_listener: Receiver<ClusterCommand>,
        aof: impl TAof,
    ) -> anyhow::Result<Self> {
        let mut consensus_con = ConsensusTracker::default();
        let mut logger = Logger::new(aof);

        while let Some(command) = cluster_message_listener.recv().await {
            // TODO notifier will be used when election process is implemented

            match command {
                ClusterCommand::AddPeer(add_peer_cmd) => {
                    self.add_peer(add_peer_cmd).await;
                }
                ClusterCommand::GetPeers(callback) => {
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
                }
                ClusterCommand::SendHeartBeat => {
                    let hop_count = self.hop_count(FANOUT, self.members.len());
                    self.send_liveness_heartbeat(hop_count).await;

                    // ! remove idle peers based on ttl.
                    // ! The following may need to be moved else where to avoid blocking the main loop
                    self.remove_idle_peers().await;
                }
                ClusterCommand::ReplicationInfo(sender) => {
                    let _ = sender.send(self.replication.clone());
                }
                ClusterCommand::SetReplicationInfo { leader_repl_id, commit_idx: offset } => {
                    self.set_replication_info(leader_repl_id, offset);
                }
                ClusterCommand::ReceiveHeartBeat(heartbeat) => {
                    if self.replication.in_ban_list(&heartbeat.heartbeat_from) {
                        continue;
                    }

                    self.gossip(heartbeat.hop_count).await;

                    if self.update_last_seen(&heartbeat.heartbeat_from).is_some() {
                        self.update_on_report(heartbeat).await;
                    }
                }
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                }
                ClusterCommand::LeaderReqConsensus { log, sender } => {
                    let append_entries =
                        logger.create_log_entries(&log, self.get_lowerest_commit_idx()).await?;

                    // Skip consensus for no replicas
                    let repl_count = self.followers().count();
                    if repl_count == 0 {
                        let _ = sender.send(None);
                        continue;
                    }

                    consensus_con.add(logger.log_index, sender, repl_count);
                    self.req_consensus(append_entries).await;
                }
                ClusterCommand::LeaderReceiveAcks(offsets) => {
                    self.apply_acks(&mut consensus_con, offsets);
                }

                ClusterCommand::SendCommitHeartBeat { offset } => {
                    self.send_commit_heartbeat(offset).await;
                }

                // * this can be called 2 different context
                // Regardless of the context, liveness update is required
                // 1. When follower is up-to-date with the leader + entry logging
                // 2. When follower is behind the leader -> entry logging + commit
                ClusterCommand::AcceptLeaderHeartBeat(heart_beat_message) => {
                    self.update_last_seen(&heart_beat_message.heartbeat_from);
                    if !heart_beat_message.append_entries.is_empty() {
                        // TODO handle the log entries
                        self.receive_log_entries_from_leader(
                            heart_beat_message.append_entries,
                            &mut logger,
                        )
                        .await;
                        continue;
                    }
                    self.replicate_state(&mut logger, heart_beat_message).await;
                }
            }
        }
        Ok(self)
    }
}
