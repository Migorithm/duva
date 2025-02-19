use futures::future::join_all;

use crate::domains::cache::cache_manager::CacheManager;
use crate::domains::query_parsers::QueryIO;
use crate::domains::ttl::manager::TtlSchedulerManager;

use std::time::SystemTime;

use super::snapshot::Snapshot;

#[derive(Clone, Debug)]
pub struct SnapshotApplier {
    cache_manager: CacheManager,
    ttl_scheduler_manager: TtlSchedulerManager,
    start_up_time: SystemTime,
}

impl SnapshotApplier {
    pub fn new(
        cache_manager: CacheManager,
        ttl_scheduler_manager: TtlSchedulerManager,
        start_up_time: SystemTime,
    ) -> Self {
        Self { cache_manager, ttl_scheduler_manager, start_up_time }
    }
    pub async fn apply_snapshot(&self, snapshot: Snapshot) -> anyhow::Result<()> {
        let ttl_inbox = self.ttl_scheduler_manager.clone();
        let startup_time = self.start_up_time;

        join_all(
            snapshot
                .key_values()
                .into_iter()
                .filter(|kvc| kvc.is_valid(&startup_time))
                .map(|kvs| self.cache_manager.route_set(kvs, ttl_inbox.clone())),
        )
        .await;

        // TODO let's find the way to test without adding the following code - echo
        // Only for debugging and test
        if let Ok(QueryIO::Array(data)) = self.cache_manager.route_keys(None).await {
            let mut keys = vec![];
            for key in data {
                let QueryIO::BulkString(key) = key else {
                    continue;
                };
                keys.push(key);
            }
            println!("[INFO] Full Sync Keys: {:?}", keys);
        }
        Ok(())
    }
}
