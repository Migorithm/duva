use crate::{
    make_smart_pointer,
    services::statefuls::routers::{cache_actor::CacheCommand, cache_dispatcher::CacheDispatcher},
};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc::Receiver, RwLock},
    time::interval,
};

pub struct TtlActor {
    pub inbox: Receiver<TtlCommand>,
    cache_dispatcher: CacheDispatcher,
}
impl TtlActor {
    pub(crate) fn run(cache_dispatcher: CacheDispatcher) -> TtlInbox {
        let (tx, inbox) = tokio::sync::mpsc::channel(100);

        Self {
            inbox,
            cache_dispatcher,
        }
        .handle();

        TtlInbox(tx)
    }

    fn handle(self) {
        let priority_queue: Arc<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>> =
            Arc::new(RwLock::new(BinaryHeap::new()));

        tokio::spawn(Self::delete_task(
            self.cache_dispatcher,
            priority_queue.clone(),
        ));
        tokio::spawn(Self::recv_task(self.inbox, priority_queue));
    }

    async fn recv_task(
        mut inbox: Receiver<TtlCommand>,
        queue: Arc<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>>,
    ) -> anyhow::Result<()> {
        while let Some(command) = inbox.recv().await {
            let Some((expire_at, key)) = command.get_expiration() else {
                break;
            };
            let mut ttl_queue = queue.write().await;
            ttl_queue.push((Reverse(expire_at), key));
        }
        Ok(())
    }

    async fn delete_task(
        cache_dispatcher: CacheDispatcher,
        queue: Arc<RwLock<BinaryHeap<(Reverse<SystemTime>, String)>>>,
    ) -> anyhow::Result<()> {
        let mut cleanup_interval = interval(Duration::from_millis(1));
        loop {
            cleanup_interval.tick().await;
            let mut queue = queue.write().await;
            while let Some((Reverse(expiry), key)) = queue.peek() {
                if expiry <= &SystemTime::now() {
                    let shard_key = cache_dispatcher.take_shard_key_from_str(key);
                    let db = &cache_dispatcher.inboxes[shard_key];
                    db.send(CacheCommand::Delete(key.clone())).await?;

                    queue.pop();
                } else {
                    break;
                }
            }
        }
    }
}

pub enum TtlCommand {
    Expiry { expiry: u64, key: String },
    StopSentinel,
}

impl TtlCommand {
    pub fn get_expiration(self) -> Option<(SystemTime, String)> {
        let (expire_in_mills, key) = match self {
            TtlCommand::Expiry { expiry, key } => (expiry, key),
            TtlCommand::StopSentinel => return None,
        };
        let expire_at = SystemTime::now() + Duration::from_millis(expire_in_mills);
        Some((expire_at, key))
    }
}

#[derive(Clone)]
pub struct TtlInbox(tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlInbox {
    pub async fn set_ttl(&self, key: String, expiry: u64) {
        let _ = self
            .send(TtlCommand::Expiry {
                key,
                expiry: expiry,
            })
            .await;
    }
}

make_smart_pointer!(TtlInbox, tokio::sync::mpsc::Sender<TtlCommand>);
