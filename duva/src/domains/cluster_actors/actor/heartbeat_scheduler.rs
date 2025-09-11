use crate::{domains::cluster_actors::actor::ClusterActorSender, types::Callback};
use std::{ops::Range, time::Duration};
use tokio::{select, time::interval};
use tracing::warn;

use super::SchedulerMessage;
const HEARTBEAT_INTERVAL: u64 = 300;
pub const ELECTION_TIMEOUT_MAX: u64 = HEARTBEAT_INTERVAL * 5;
const ELECTION_TIMEOUT: Range<u64> = HEARTBEAT_INTERVAL * 3..ELECTION_TIMEOUT_MAX;

#[derive(Debug)]
pub(crate) struct HeartBeatScheduler {
    cluster_handler: ClusterActorSender,
    controller: Option<SchedulerMode>,
}

impl HeartBeatScheduler {
    pub(crate) fn run(
        cluster_handler: ClusterActorSender,
        is_leader_mode: bool,
        cluster_heartbeat_interval: u64,
    ) -> Self {
        let controller = if is_leader_mode {
            SchedulerMode::Leader(Self::send_append_entries_rpc(
                HEARTBEAT_INTERVAL,
                cluster_handler.clone(),
            ))
        } else {
            SchedulerMode::Follower(Self::start_election_timer(cluster_handler.clone()))
        };

        Self { cluster_handler, controller: Some(controller) }
            .send_cluster_heartbeat(cluster_heartbeat_interval)
    }

    pub(crate) fn send_cluster_heartbeat(self, cluster_heartbeat_interval: u64) -> Self {
        let handler: ClusterActorSender = self.cluster_handler.clone();
        let mut heartbeat_interval = interval(Duration::from_millis(cluster_heartbeat_interval));

        tokio::spawn({
            async move {
                loop {
                    heartbeat_interval.tick().await;
                    let _ = handler.send(SchedulerMessage::SendPeriodicHeatBeat).await;
                }
            }
        });
        self
    }

    pub(crate) fn send_append_entries_rpc(
        heartbeat_interval: u64,
        cluster_handler: ClusterActorSender,
    ) -> Callback<()> {
        let (callback, rx) = Callback::create();

        let mut itv = interval(Duration::from_millis(heartbeat_interval));

        tokio::spawn(async move {
            select! {

                _ = rx.recv() => {},
                _ = async {
                    loop {
                        itv.tick().await;
                        let _ = cluster_handler.send(SchedulerMessage::SendAppendEntriesRPC).await;
                    }
                } => {},
            }
        });

        callback
    }

    pub(crate) fn start_election_timer(
        cluster_handler: ClusterActorSender,
    ) -> tokio::sync::mpsc::Sender<ElectionTimeOutCommand> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ElectionTimeOutCommand>(5);

        tokio::spawn(async move {
            loop {
                select! {
                    biased;
                    message = rx.recv() => {
                        let Some(msg) = message else {
                            return
                        };
                        match msg {
                            ElectionTimeOutCommand::Stop => return,
                            ElectionTimeOutCommand::Ping => {},
                        }
                    },

                    _ =  tokio::time::sleep(Duration::from_millis(rand::random_range(ELECTION_TIMEOUT)))=>{
                        warn!("\x1b[33mElection timeout\x1b[0m");
                        let _ = cluster_handler.send(SchedulerMessage::StartLeaderElection).await;

                    }
                }
            }
        });

        tx
    }

    pub(crate) fn reset_election_timeout(&self) {
        if let Some(SchedulerMode::Follower(tx)) = &self.controller {
            let _ = tx.try_send(ElectionTimeOutCommand::Ping);
        }
    }

    pub(crate) async fn turn_leader_mode(&mut self) {
        let controller = match self.controller.take() {
            | Some(SchedulerMode::Follower(sender)) => {
                let _ = sender.send(ElectionTimeOutCommand::Stop).await;
                Some(SchedulerMode::Leader(Self::send_append_entries_rpc(
                    HEARTBEAT_INTERVAL,
                    self.cluster_handler.clone(),
                )))
            },
            | Some(SchedulerMode::Leader(sender)) => Some(SchedulerMode::Leader(sender)),
            | None => None,
        };
        self.controller = controller;
    }

    pub(crate) async fn turn_follower_mode(&mut self) {
        let controller = match self.controller.take() {
            | Some(SchedulerMode::Leader(sender)) => {
                sender.send(());
                Some(SchedulerMode::Follower(Self::start_election_timer(
                    self.cluster_handler.clone(),
                )))
            },
            | Some(SchedulerMode::Follower(sender)) => Some(SchedulerMode::Follower(sender)),
            | None => None,
        };
        self.controller = controller;
    }
}

pub enum ElectionTimeOutCommand {
    Stop,
    Ping,
}

#[derive(Debug)]
enum SchedulerMode {
    Leader(Callback<()>),
    Follower(tokio::sync::mpsc::Sender<ElectionTimeOutCommand>),
}

#[cfg(test)]
mod tests {
    use crate::domains::cluster_actors::ClusterCommand;
    use crate::domains::cluster_actors::queue::{ClusterActorQueue, ClusterActorReceiver};

    use super::*;

    use tokio::time::{Duration, timeout};

    // Helper function to create a test scheduler
    async fn setup_scheduler(master_mode: bool) -> (HeartBeatScheduler, ClusterActorReceiver) {
        let (sender, rx) = ClusterActorQueue::create(10);
        let scheduler = HeartBeatScheduler::run(sender, master_mode, 200);
        (scheduler, rx)
    }

    #[tokio::test]
    async fn test_leader_mode_initialization() {
        let (scheduler, _) = setup_scheduler(true).await;

        match scheduler.controller {
            | Some(SchedulerMode::Leader(_)) => assert!(true),
            | Some(SchedulerMode::Follower(_)) => assert!(false, "Expected Leader mode"),
            | None => assert!(false, "Expected Leader mode"),
        }
    }

    #[tokio::test]
    async fn test_follower_mode_initialization() {
        let (scheduler, _) = setup_scheduler(false).await;

        match scheduler.controller {
            | Some(SchedulerMode::Follower(_)) => assert!(true),
            | Some(SchedulerMode::Leader(_)) => assert!(false, "Expected Follower mode"),
            | None => todo!(),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_periodically() {
        let (_, mut rx) = setup_scheduler(true).await;

        // Wait for at least 2 heartbeats
        let received = timeout(Duration::from_millis(500), async {
            let mut count = 0;
            while let Some(cmd) = rx.recv().await {
                assert!(matches!(
                    cmd,
                    ClusterCommand::Scheduler(SchedulerMessage::SendAppendEntriesRPC)
                        | ClusterCommand::Scheduler(SchedulerMessage::SendPeriodicHeatBeat)
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
        let (tx, mut rx) = ClusterActorQueue::create(10);
        let heartbeat_interval = 100; // 100ms
        let stop_signal =
            HeartBeatScheduler::send_append_entries_rpc(heartbeat_interval, tx.into());

        // Wait for at least 2 heartbeats
        let received = timeout(Duration::from_millis(250), async {
            let mut count = 0;
            while let Some(cmd) = rx.recv().await {
                assert!(matches!(
                    cmd,
                    ClusterCommand::Scheduler(SchedulerMessage::SendAppendEntriesRPC)
                ));
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
        let (tx, _) = ClusterActorQueue::create(10);
        let controller = HeartBeatScheduler::start_election_timer(tx);

        // Test stopping the election timeout
        let stop_result = timeout(Duration::from_millis(100), async {
            controller.send(ElectionTimeOutCommand::Stop).await
        })
        .await
        .expect("Timeout waiting for stop");

        assert!(stop_result.is_ok(), "Should be able to send stop command");

        // Test election trigger after timeout
        let (tx2, mut rx2) = ClusterActorQueue::create(10);
        // ! Keep the sender alive! otherwise, the receiver will close immediately
        let _sender = HeartBeatScheduler::start_election_timer(tx2);

        let election_triggered =
            timeout(Duration::from_millis(HEARTBEAT_INTERVAL * 6), async { rx2.recv().await })
                .await
                .expect("Timeout waiting for election");

        assert!(
            matches!(
                election_triggered,
                Some(ClusterCommand::Scheduler(SchedulerMessage::StartLeaderElection))
            ),
            "Should trigger election after timeout"
        );
    }

    #[tokio::test]
    async fn test_update_leader_heartbeat() {
        let (tx, _rx) = ClusterActorQueue::create(10);
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
