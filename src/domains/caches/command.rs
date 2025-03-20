use tokio::sync::{mpsc, oneshot};

use crate::domains::{query_parsers::QueryIO, saves::command::SaveCommand};

use super::cache_objects::CacheEntry;

pub enum CacheCommand {
    Set { cache_entry: CacheEntry },
    Save { outbox: mpsc::Sender<SaveCommand> },
    Get { key: String, sender: oneshot::Sender<QueryIO> },
    Keys { pattern: Option<String>, sender: oneshot::Sender<QueryIO> },
    Delete(String),

    StopSentinel,
    IndexGet { key: String, index: u64, sender: oneshot::Sender<QueryIO> },
}
