use crate::services::value::Value;

use super::{ttl_handlers::set::TtlSetter, CacheDb};

use tokio::sync::oneshot;

pub enum CacheCommand {
    Set {
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlSetter,
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

pub enum AOFCommand {
    Set {
        key: String,
        value: String,
        expiry: Option<u64>,
    },
    Delete(String),
}
