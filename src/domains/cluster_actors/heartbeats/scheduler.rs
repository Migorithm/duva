use std::time::Duration;

use tokio::{select, sync::mpsc::Sender, time::interval};

use crate::domains::cluster_actors::commands::ClusterCommand;

const LEADER_HEARTBEAT_INTERVAL: u64 = 300;

pub(crate) struct HeartBeatScheduler {
    cluster_handler: Sender<ClusterCommand>,
    follower_mode_controller: Option<tokio::sync::oneshot::Sender<()>>,
    leader_mode_controller: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HeartBeatScheduler {
    pub(crate) fn heartbeat_periodically(&self, cluster_heartbeat_interval: u64) {
        let handler = self.cluster_handler.clone();
        let mut heartbeat_interval = interval(Duration::from_millis(cluster_heartbeat_interval));

        tokio::spawn({
            async move {
                loop {
                    heartbeat_interval.tick().await;
                    let _ = handler.send(ClusterCommand::SendHeartBeat).await;
                }
            }
        });
    }

    pub(crate) fn leader_heartbeat_periodically(
        &self,
        master_mode: bool,
        heartbeat_interval: u64,
    ) -> Option<tokio::sync::oneshot::Sender<()>> {
        if !master_mode {
            return None;
        }
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handler = self.cluster_handler.clone();
        let mut itv = interval(Duration::from_millis(heartbeat_interval));

        tokio::spawn(async move {
            select! {
                _ = rx => {},
                _ = async {
                    loop {
                        itv.tick().await;
                        let _ = handler.send(ClusterCommand::SendLeaderHeartBeat).await;
                    }
                } => {},
            }
        });

        Some(tx)
    }

    pub(crate) fn schedule_election_timeout(&self) -> tokio::sync::oneshot::Sender<()> {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handler = self.cluster_handler.clone();
        tokio::spawn(async move {
            select! {
                    _ = rx => {},
                    _ = tokio::time::sleep(
                        // randomize the election timeout to avoid split votes
                        Duration::from_millis(rand::random_range(800..1200))

                    ) => {
                        let _ = handler.send(ClusterCommand::StartLeaderElection).await;
                    },
            }
        });

        tx
    }
}

pub enum HeartBeatSchedulerCommand {
    Stop,
}
