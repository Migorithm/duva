use crate::{services::value::Value, services::value::Values};

use super::ttl_handlers::set::TtlSetter;

use tokio::sync::oneshot;

pub enum PersistCommand {
    Set(Values, TtlSetter),
    Get(Values, oneshot::Sender<Value>),
    Delete(String),
    StopSentinel,
}
