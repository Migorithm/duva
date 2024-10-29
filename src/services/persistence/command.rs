use crate::services::value::Value;

use super::ttl_handlers::set::TtlSetter;

use tokio::sync::oneshot;

pub enum PersistCommand {
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
    Delete(String),
    StopSentinel,
}
