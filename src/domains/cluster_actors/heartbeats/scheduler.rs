use std::time::Duration;

use tokio::{select, sync::mpsc::Sender, time::interval};

use crate::domains::cluster_actors::commands::ClusterCommand;

const LEADER_HEARTBEAT_INTERVAL: u64 = 300;

#[derive(Debug)]
pub(crate) struct HeartBeatScheduler {
    cluster_handler: Sender<ClusterCommand>,
    controller: ModeController,
}

impl HeartBeatScheduler {
    pub fn run(
        cluster_handler: Sender<ClusterCommand>,
        is_leader_mode: bool,
        cluster_heartbeat_interval: u64,
    ) -> Self {
        let controller = if is_leader_mode {
            ModeController::Leader(Self::leader_heartbeat_periodically(
                LEADER_HEARTBEAT_INTERVAL,
                cluster_handler.clone(),
            ))
        } else {
            ModeController::Follower(Self::start_election_timer(cluster_handler.clone()))
        };

        Self { cluster_handler, controller }.heartbeat_periodically(cluster_heartbeat_interval)
    }

    pub(crate) fn heartbeat_periodically(self, cluster_heartbeat_interval: u64) -> Self {
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
        self
    }

    pub(crate) fn leader_heartbeat_periodically(
        heartbeat_interval: u64,
        cluster_handler: Sender<ClusterCommand>,
    ) -> tokio::sync::oneshot::Sender<()> {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let mut itv = interval(Duration::from_millis(heartbeat_interval));

        tokio::spawn(async move {
            select! {
                _ = rx => {},
                _ = async {
                    loop {
                        itv.tick().await;
                        let _ = cluster_handler.send(ClusterCommand::SendLeaderHeartBeat).await;
                    }
                } => {},
            }
        });

        tx
    }

    pub(crate) fn start_election_timer(
        cluster_handler: Sender<ClusterCommand>,
    ) -> tokio::sync::mpsc::Sender<ElectionTimeOutCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ElectionTimeOutCommand>(5);

        tokio::spawn(async move {
            loop {
                select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            ElectionTimeOutCommand::Stop => return,
                            ElectionTimeOutCommand::Ping => {},
                        }
                    },
                    _ =  tokio::time::sleep(Duration::from_millis(rand::random_range(LEADER_HEARTBEAT_INTERVAL*3..LEADER_HEARTBEAT_INTERVAL*5)))=>{
                        let _ = cluster_handler.send(ClusterCommand::StartLeaderElection).await;

                    }
                }
            }
        });

        tx
    }
}

pub enum ElectionTimeOutCommand {
    Stop,
    Ping,
}

#[derive(Debug)]
enum ModeController {
    Leader(tokio::sync::oneshot::Sender<()>),
    Follower(tokio::sync::mpsc::Sender<ElectionTimeOutCommand>),
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::mpsc::{Receiver, channel};
    use tokio::time::{Duration, timeout};

    // Helper function to create a test scheduler
    async fn setup_scheduler(master_mode: bool) -> (HeartBeatScheduler, Receiver<ClusterCommand>) {
        let (tx, rx) = channel(10);
        let scheduler = HeartBeatScheduler::run(tx, master_mode, 200);
        (scheduler, rx)
    }

    #[tokio::test]
    async fn test_leader_mode_initialization() {
        let (scheduler, _) = setup_scheduler(true).await;

        match scheduler.controller {
            ModeController::Leader(_) => assert!(true),
            ModeController::Follower(_) => assert!(false, "Expected Leader mode"),
        }
    }

    #[tokio::test]
    async fn test_follower_mode_initialization() {
        let (scheduler, _) = setup_scheduler(false).await;

        match scheduler.controller {
            ModeController::Follower(_) => assert!(true),
            ModeController::Leader(_) => assert!(false, "Expected Follower mode"),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_periodically() {
        let (scheduler, mut rx) = setup_scheduler(true).await;

        // Wait for at least 2 heartbeats
        let received = timeout(Duration::from_millis(250), async {
            let mut count = 0;
            while let Some(cmd) = rx.recv().await {
                assert!(matches!(
                    cmd,
                    ClusterCommand::SendLeaderHeartBeat | ClusterCommand::SendHeartBeat
                ));
                count += 1;
                if count >= 2 {
                    break;
                }
            }
            count
        })
        .await
        .expect("Timeout waiting for heartbeats");

        assert!(received >= 2, "Should receive at least 2 heartbeats");
    }

    #[tokio::test]
    async fn test_leader_heartbeat_periodically() {
        let (tx, mut rx) = channel(10);
        let heartbeat_interval = 100; // 100ms
        let stop_signal = HeartBeatScheduler::leader_heartbeat_periodically(heartbeat_interval, tx);

        // Wait for at least 2 heartbeats
        let received = timeout(Duration::from_millis(250), async {
            let mut count = 0;
            while let Some(cmd) = rx.recv().await {
                assert!(matches!(cmd, ClusterCommand::SendLeaderHeartBeat));
                count += 1;
                if count >= 2 {
                    break;
                }
            }
            count
        })
        .await
        .expect("Timeout waiting for leader heartbeats");

        // Stop the heartbeat
        let _ = stop_signal.send(());

        assert!(received >= 2, "Should receive at least 2 leader heartbeats");
    }

    #[tokio::test]
    async fn test_election_timeout() {
        let (tx, mut rx) = channel(10);
        let controller = HeartBeatScheduler::start_election_timer(tx);

        // Test stopping the election timeout
        let stop_result = timeout(Duration::from_millis(100), async {
            controller.send(ElectionTimeOutCommand::Stop).await
        })
        .await
        .expect("Timeout waiting for stop");

        assert!(stop_result.is_ok(), "Should be able to send stop command");

        // Test election trigger after timeout
        let (tx2, mut rx2) = channel(10);
        let _ = HeartBeatScheduler::start_election_timer(tx2);

        let election_triggered =
            timeout(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL * 6), async {
                rx2.recv().await
            })
            .await
            .expect("Timeout waiting for election");

        assert!(
            matches!(election_triggered, Some(ClusterCommand::StartLeaderElection)),
            "Should trigger election after timeout"
        );
    }

    #[tokio::test]
    async fn test_update_leader_heartbeat() {
        let (tx, _rx) = channel(10);
        let controller = HeartBeatScheduler::start_election_timer(tx);

        // Test sending UpdateLeaderHeartBeat command
        let ping_result = timeout(Duration::from_millis(100), async {
            controller.send(ElectionTimeOutCommand::Ping).await
        })
        .await
        .expect("Timeout waiting for update");

        assert!(ping_result.is_ok(), "Should be able to send update command");
    }
}
