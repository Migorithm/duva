use super::{super::query_manager::value::Value, ttl_handlers};

use tokio::sync::{mpsc, oneshot};
use ttl_handlers::command::TtlCommand;

use super::super::query_manager::query::Args;

#[derive(Debug)]
pub enum PersistCommand {
    Set(Args, mpsc::Sender<TtlCommand>),
    Get(Args, oneshot::Sender<Value>),
    Delete(String),
    StopSentinel,
}
