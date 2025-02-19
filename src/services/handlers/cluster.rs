use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::consensus::ConsensusTracker;
use crate::domains::cluster_actors::{ClusterActor, FANOUT};

use tokio::sync::mpsc::Receiver;

impl ClusterActor {
    pub(crate) async fn handle(
        mut self,
        mut cluster_message_listener: Receiver<ClusterCommand>,
    ) -> anyhow::Result<Self> {
        let mut consensus_con = ConsensusTracker::default();

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
                ClusterCommand::SetReplicationInfo { leader_repl_id, offset } => {
                    self.set_replication_info(leader_repl_id, offset);
                }
                ClusterCommand::ReceiveHeartBeat(heartbeat) => {
                    if self.replication.in_ban_list(&heartbeat.heartbeat_from) {
                        continue;
                    }

                    self.gossip(heartbeat.hop_count).await;
                    self.update_on_report(heartbeat).await;
                }
                ClusterCommand::ForgetPeer(peer_addr, sender) => {
                    if let Ok(Some(())) = self.forget_peer(peer_addr).await {
                        let _ = sender.send(Some(()));
                    } else {
                        let _ = sender.send(None);
                    }
                }
                ClusterCommand::LeaderReqConsensus { log, sender } => {
                    // ! If there are no replicas, don't send the request
                    if self.followers().count() == 0 {
                        let _ = sender.send(None);
                        continue;
                    }
                    self.req_consensus(log).await;

                    consensus_con.add(
                        self.replication.leader_repl_offset,
                        sender,
                        self.followers().count(),
                    );
                }
                ClusterCommand::LeaderReceiveAcks(offsets) => {
                    self.apply_acks(&mut consensus_con, offsets);
                }
                ClusterCommand::FollowerReceiveLogEntries(write_operations) => {
                    // TODO handle the log entries
                    println!("[INFO] Received log entries: {:?}", write_operations);
                    self.receive_log_entries_from_leader(write_operations).await;
                }
            }
        }
        Ok(self)
    }
}
