use super::{super::controller::value::Value, ttl_handlers::set::TtlSetter};

use tokio::sync::oneshot;

use super::super::controller::query::Args;

pub enum PersistCommand {
    Set(Args, TtlSetter),
    Get(Args, oneshot::Sender<Value>),
    Delete(String),
    StopSentinel,
}
