use super::{super::query_manager::value::Value, ttl_handlers::set::TtlSetter};

use tokio::sync::oneshot;

use super::super::query_manager::query::Args;

pub enum PersistCommand {
    Set(Args, TtlSetter),
    Get(Args, oneshot::Sender<Value>),
    Delete(String),
    StopSentinel,
}
