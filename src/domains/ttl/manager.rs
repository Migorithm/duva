use crate::make_smart_pointer;

use std::time::SystemTime;

use super::command::TtlCommand;

#[derive(Clone, Debug)]
pub struct TtlSchedulerManager(pub(crate) tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlSchedulerManager {
    pub async fn set_ttl(&self, key: String, expiry: SystemTime) {
        let _ = self.send(TtlCommand::ScheduleTtl { key, expiry }).await;
    }
}

make_smart_pointer!(TtlSchedulerManager, tokio::sync::mpsc::Sender<TtlCommand>);
