use tokio::sync::{mpsc, oneshot};

use crate::domains::{query_parsers::QueryIO, saves::command::SaveCommand};

use super::cache_objects::CacheEntry;

pub enum CacheCommand {
    Set { cache_entry: CacheEntry },
    Save { outbox: mpsc::Sender<SaveCommand> },
    Get { key: String, callback: oneshot::Sender<Option<String>> },
    Keys { pattern: Option<String>, callback: oneshot::Sender<QueryIO> },
    Delete { key: String, callback: oneshot::Sender<bool> },
    IndexGet { key: String, read_idx: u64, callback: oneshot::Sender<Option<String>> },
    Ping,
    StopSentinel,
    Drop { callback: oneshot::Sender<()> },
    Exists { key: String, callback: oneshot::Sender<bool> },
}
