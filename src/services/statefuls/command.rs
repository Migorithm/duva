use crate::services::value::Value;

use super::{routers::cache_actor::CacheDb, ttl_handlers::set::TtlInbox};

use tokio::sync::oneshot;

pub enum CacheCommand {
    Set {
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlInbox,
    },
    Get {
        key: String,
        sender: oneshot::Sender<Value>,
    },
    Keys {
        pattern: Option<String>,
        sender: oneshot::Sender<Value>,
    },
    Delete(String),
    StartUp(CacheDb),
    StopSentinel,
}
