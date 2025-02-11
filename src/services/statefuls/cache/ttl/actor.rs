use crate::services::statefuls::cache::actor::CacheCommand;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::command::TtlCommand;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerManager;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time::interval;

pub struct TtlActor;

impl TtlActor {
    pub(crate) fn run(cache_dispatcher: CacheManager) -> TtlSchedulerManager {
        let (scheduler_outbox, inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(Self::ttl_schedule_actor(inbox));
        tokio::spawn(Self::background_delete_actor(cache_dispatcher, scheduler_outbox.clone()));

        TtlSchedulerManager(scheduler_outbox)
    }

    // Background actor keeps sending peek command to the scheduler actor to check if there is any key to delete.
    async fn background_delete_actor(
        cache_manager: CacheManager,
        outbox: mpsc::Sender<TtlCommand>,
    ) -> anyhow::Result<()> {
        let mut cleanup_interval = interval(Duration::from_millis(1));
        loop {
            cleanup_interval.tick().await;
            let (tx, rx) = tokio::sync::oneshot::channel();
            outbox.send(TtlCommand::Peek(tx)).await?;

            let Ok(Some(key)) = rx.await else {
                continue;
            };
            cache_manager.select_shard(&key).send(CacheCommand::Delete(key)).await?;
        }
    }

    async fn ttl_schedule_actor(mut inbox: Receiver<TtlCommand>) -> anyhow::Result<()> {
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
