use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerManager;
use crate::services::statefuls::snapshot::snapshot::Snapshot;
use std::time::SystemTime;

#[derive(Clone)]
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
        self.cache_manager
            .apply_snapshot(snapshot, self.ttl_scheduler_manager.clone(), self.start_up_time)
            .await?;

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
