use crate::{
    make_smart_pointer,
    services::{
        interfaces::endec::TDecodeData,
        statefuls::routers::{cache_actor::CacheCommand, cache_manager::CacheManager},
    },
};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{
        mpsc::{self, Receiver},
        oneshot,
    },
    time::interval,
};

pub struct TtlActor;

impl TtlActor {
    pub(crate) fn run<T: TDecodeData>(cache_dispatcher: CacheManager<T>) -> TtlSchedulerInbox {
        let (scheduler_outbox, inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(Self::ttl_schedule_actor(inbox));
        tokio::spawn(Self::background_delete_actor(
            cache_dispatcher,
            scheduler_outbox.clone(),
        ));

        TtlSchedulerInbox(scheduler_outbox)
    }

    // Background actor keeps sending peek command to the scheduler actor to check if there is any key to delete.
    async fn background_delete_actor<T: TDecodeData>(
        cache_manager: CacheManager<T>,
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
            cache_manager
                .select_shard(&key)
                .send(CacheCommand::Delete(key))
                .await?;
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
                        sender
                            .send(None)
                            .map_err(|_| anyhow::anyhow!("Error sending key"))?;
                    }
                },

                TtlCommand::StopSentinel => break,
            }
        }
        Ok(())
    }
}

pub enum TtlCommand {
    ScheduleTtl { expiry: SystemTime, key: String },
    Peek(oneshot::Sender<Option<String>>),
    StopSentinel,
}

impl TtlCommand {
    pub fn get_expiration(self) -> Option<(SystemTime, String)> {
        let (expire_in_mills, key) = match self {
            TtlCommand::ScheduleTtl { expiry, key } => (expiry, key),
            _ => return None,
        };

        Some((expire_in_mills, key))
    }
}

#[derive(Clone)]
pub struct TtlSchedulerInbox(tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlSchedulerInbox {
    pub async fn set_ttl(&self, key: String, expiry: SystemTime) {
        let _ = self.send(TtlCommand::ScheduleTtl { key, expiry }).await;
    }
}

make_smart_pointer!(TtlSchedulerInbox, tokio::sync::mpsc::Sender<TtlCommand>);
