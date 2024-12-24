use crate::make_smart_pointer;
use crate::services::statefuls::cache::ttl::command::TtlCommand;
use std::time::SystemTime;

#[derive(Clone)]
pub struct TtlSchedulerInbox(pub(crate) tokio::sync::mpsc::Sender<TtlCommand>);

impl TtlSchedulerInbox {
    pub async fn set_ttl(&self, key: String, expiry: SystemTime) {
        let _ = self.send(TtlCommand::ScheduleTtl { key, expiry }).await;
    }
}

make_smart_pointer!(TtlSchedulerInbox, tokio::sync::mpsc::Sender<TtlCommand>);
