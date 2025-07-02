use super::cache_objects::{CacheEntry, CacheValue};
use crate::domains::saves::command::SaveCommand;
use tokio::sync::{mpsc, oneshot};

pub(crate) enum CacheCommand {
    Set { cache_entry: CacheEntry },
    Save { outbox: mpsc::Sender<SaveCommand> },
    Get { key: String, callback: oneshot::Sender<CacheValue> },
    Keys { pattern: Option<String>, callback: oneshot::Sender<Vec<String>> },
    Delete { key: String, callback: oneshot::Sender<bool> },
    IndexGet { key: String, read_idx: u64, callback: oneshot::Sender<CacheValue> },
    Ping,
    Drop { callback: oneshot::Sender<()> },
    Exists { key: String, callback: oneshot::Sender<bool> },
    Append { key: String, value: String, callback: oneshot::Sender<anyhow::Result<usize>> },
    NumericDetla { key: String, delta: i64, callback: oneshot::Sender<anyhow::Result<i64>> },
}
