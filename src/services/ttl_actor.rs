use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, SystemTime},
};

use tokio::{sync::mpsc::Receiver, time::interval};

use crate::domains::{
    storage::command::CacheCommand,
    ttl::{actor::TtlActor, command::TtlCommand, manager::TtlSchedulerManager},
};

impl TtlActor {
    pub(crate) fn run(self) -> TtlSchedulerManager {
        let (scheduler_outbox, inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(Self::handle(inbox));
        tokio::spawn(self.background_delete_actor(scheduler_outbox.clone()));

        TtlSchedulerManager(scheduler_outbox)
    }

    // Background actor keeps sending peek command to the scheduler actor to check if there is any key to delete.
    async fn background_delete_actor(
        self,
        outbox: tokio::sync::mpsc::Sender<TtlCommand>,
    ) -> anyhow::Result<()> {
        let mut cleanup_interval = interval(Duration::from_millis(1));
        loop {
            cleanup_interval.tick().await;
            let (tx, rx) = tokio::sync::oneshot::channel();
            outbox.send(TtlCommand::Peek(tx)).await?;

            let Ok(Some(key)) = rx.await else {
                continue;
            };
            self.select_shard(&key).send(CacheCommand::Delete(key)).await?;
        }
    }
    async fn handle(mut inbox: Receiver<TtlCommand>) -> anyhow::Result<()> {
        let mut priority_queue: BinaryHeap<(Reverse<SystemTime>, String)> = BinaryHeap::new();

        while let Some(cmd) = inbox.recv().await {
            match cmd {
                TtlCommand::ScheduleTtl { expiry, key } => {
                    priority_queue.push((Reverse(expiry), key));
                }

                TtlCommand::Peek(sender) => match priority_queue.peek() {
                    Some((Reverse(expiry), key)) if expiry <= &SystemTime::now() => {
                        sender
                            .send(Some(key.clone()))
                            .map_err(|_| anyhow::anyhow!("Error sending key"))?;

                        priority_queue.pop();
                    }
                    _ => {
                        sender.send(None).map_err(|_| anyhow::anyhow!("Error sending key"))?;
                    }
                },

                TtlCommand::StopSentinel => break,
            }
        }
        Ok(())
    }
}
