use crate::make_smart_pointer;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use crate::services::statefuls::snapshot::actor::SnapshotActor;
use crate::services::statefuls::snapshot::command::SnapshotCommand;
use crate::services::statefuls::snapshot::dump_file::DumpFile;
use std::time::SystemTime;
use tokio::sync::mpsc::Sender;

make_smart_pointer!(SnapshotManager, Sender<SnapshotCommand> => outbox);

#[derive(Clone)]
pub struct SnapshotManager {
    outbox: Sender<SnapshotCommand>,
}

impl SnapshotManager {
    pub fn new(
        cache_manager: &'static CacheManager,
        ttl_inbox: TtlSchedulerInbox,
        start_up_time: SystemTime,
    ) -> Self {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(SnapshotActor::new(cache_manager, ttl_inbox.clone(), start_up_time, inbox).handle());
        Self {
            outbox,
        }
    }

    pub async fn replace_snapshot(&self, dump_file: DumpFile) -> anyhow::Result<()> {
        self.send(SnapshotCommand::ReplaceSnapshot(dump_file)).await.map_err(Into::into)
    }
}