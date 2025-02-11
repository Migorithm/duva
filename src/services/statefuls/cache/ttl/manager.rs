use crate::make_smart_pointer;
use crate::services::statefuls::cache::ttl::command::TtlCommand;
use std::time::SystemTime;

#[derive(Clone)]
pub struct TtlSchedulerManager(pub(crate) tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlSchedulerManager {
    pub async fn set_ttl(&self, key: String, expiry: SystemTime) {
        let _ = self.send(TtlCommand::ScheduleTtl { key, expiry }).await;
    }
}

make_smart_pointer!(TtlSchedulerManager, tokio::sync::mpsc::Sender<TtlCommand>);
