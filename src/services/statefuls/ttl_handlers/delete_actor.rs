use super::ttl_queue;
use crate::services::statefuls::{
    command::CacheCommand, routers::cache_dispatcher::CacheDispatcher,
};
use anyhow::Result;
use std::{
    cmp::Reverse,
    time::{Duration, SystemTime},
};
use tokio::time::interval;

pub(crate) struct TtlDeleteActor {
    pub cache_dispatcher: CacheDispatcher,
}

impl TtlDeleteActor {
    pub fn run(cache_dispatcher: &CacheDispatcher) {
        tokio::spawn(
            Self {
                cache_dispatcher: cache_dispatcher.clone(),
            }
            .handle(),
        );
    }

    async fn handle(self) -> Result<()> {
        let mut cleanup_interval = interval(Duration::from_millis(1));
        loop {
            cleanup_interval.tick().await;
            let mut queue = ttl_queue().write().await;
            while let Some((Reverse(expiry), key)) = queue.peek() {
                if expiry <= &SystemTime::now() {
                    let shard_key = self.cache_dispatcher.take_shard_key_from_str(key);
                    let db = &self.cache_dispatcher.inboxes[shard_key];
                    db.send(CacheCommand::Delete(key.clone())).await?;

                    queue.pop();
                } else {
                    break;
                }
            }
        }
    }
}
