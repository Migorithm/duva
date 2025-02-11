use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use crate::services::statefuls::snapshot::command::SnapshotCommand;
use std::time::SystemTime;
use tokio::sync::mpsc::Receiver;
use crate::services::query_io::QueryIO;

pub struct SnapshotActor {
    cache_manager: &'static CacheManager,
    ttl_inbox: TtlSchedulerInbox,
    startup_time: SystemTime,
    inbox: Receiver<SnapshotCommand>,
}

impl SnapshotActor {
    pub fn new(
        cache_manager: &'static CacheManager,
        ttl_inbox: TtlSchedulerInbox,
        startup_time: SystemTime,
        inbox: Receiver<SnapshotCommand>,
    ) -> Self {
        Self { cache_manager, ttl_inbox, startup_time, inbox }
    }
    pub async fn handle(mut self) -> anyhow::Result<()> {
        while let Some(command) = self.inbox.recv().await {
            match command {
                SnapshotCommand::ReplaceSnapshot(dump) => {
                    self.cache_manager
                        .dump_cache(dump, self.ttl_inbox.clone(), self.startup_time)
                        .await?;

                    if let Ok(query_io) = self.cache_manager.route_keys(None).await {
                        println!("Keys {:#?}",query_io)
                    }
                }
            }
        }
        Ok(())
    }
}
