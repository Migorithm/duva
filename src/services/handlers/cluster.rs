use crate::domains::append_only_files::interfaces::TAof;
use crate::domains::append_only_files::logger::Logger;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::replication::ReplicationInfo;
use crate::domains::cluster_actors::{ClusterActor, FANOUT};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::interval;

impl ClusterActor {
    pub(crate) async fn handle(
        mut self,
        mut listener: Receiver<ClusterCommand>,
        aof: impl TAof,
        cache_manager: CacheManager,
        heartbeat_fq_mills: u64,
        self_handler: Sender<ClusterCommand>,
    ) -> anyhow::Result<Self> {
        let mut logger = Logger::new(aof);
        Self::heartbeat_periodically(heartbeat_fq_mills, self_handler.clone());
        let mut _leader_mode_hb_sender = Self::leader_heartbeat_periodically(
            300,
            self_handler,
            self.replication.is_leader_mode(),
        );

        while let Some(command) = listener.recv().await {
            // TODO notifier will be used when election process is implemented

            match command {
                ClusterCommand::AddPeer(add_peer_cmd) => {
                    self.add_peer(add_peer_cmd).await;
                },
                ClusterCommand::GetPeers(callback) => {
                    let _ = callback.send(self.members.keys().cloned().collect::<Vec<_>>().into());
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
                ClusterCommand::ReceiveHeartBeat(heartbeat) => {
                    if self.replication.in_ban_list(&heartbeat.heartbeat_from) {
                        continue;
                    }

                    self.gossip(heartbeat.hop_count).await;

                    if self.update_last_seen(&heartbeat.heartbeat_from).is_some() {
                        self.update_on_report(heartbeat).await;
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
                ClusterCommand::AcceptLeaderHeartBeat(heart_beat_message) => {
                    self.update_last_seen(&heart_beat_message.heartbeat_from);
                    self.replicate(&mut logger, heart_beat_message, &cache_manager).await;
                },
                ClusterCommand::SendLeaderHeartBeat => {
                    self.send_leader_heartbeat().await;
                },
            }
        }
        Ok(self)
    }

    pub fn heartbeat_periodically(heartbeat_fq_mills: u64, actor_handler: Sender<ClusterCommand>) {
        tokio::spawn(async move {
            let mut heartbeat_interval = interval(Duration::from_millis(heartbeat_fq_mills));
            loop {
                heartbeat_interval.tick().await;
                let _ = actor_handler.send(ClusterCommand::SendHeartBeat).await;
            }
        });
    }
    pub fn leader_heartbeat_periodically(
        heartbeat_fq_mills: u64,
        actor_handler: Sender<ClusterCommand>,
        is_leader_mode: bool,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !is_leader_mode {
            return None;
        }
        Some(tokio::spawn(async move {
            let mut heartbeat_interval = interval(Duration::from_millis(heartbeat_fq_mills));
            loop {
                heartbeat_interval.tick().await;
                let _ = actor_handler.send(ClusterCommand::SendLeaderHeartBeat).await;
            }
        }))
    }

    pub fn run(
        node_timeout: u128,
        heartbeat_fq_mills: u64,
        init_replication: ReplicationInfo,
        cache_manager: CacheManager,
        notifier: tokio::sync::watch::Sender<bool>,
        aof: impl TAof,
    ) -> Sender<ClusterCommand> {
        let (actor_handler, cluster_message_listener) = tokio::sync::mpsc::channel(100);
        tokio::spawn(ClusterActor::new(node_timeout, init_replication, notifier).handle(
            cluster_message_listener,
            aof,
            cache_manager,
            heartbeat_fq_mills,
            actor_handler.clone(),
        ));
        actor_handler
    }
}

#[tokio::test]
async fn test_you_can_kill_task_that_runs_infinite_loop() {
    let task = tokio::spawn(async move {
        loop {
            println!("Task is running");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(1)).await;
    task.abort();

    println!("Task is killed");
}
