use tokio::sync::{mpsc, oneshot};

use crate::{
    domains::{save::command::SaveCommand, ttl::manager::TtlSchedulerManager},
    services::query_io::QueryIO,
};

use super::cache_objects::CacheEntry;

pub enum CacheCommand {
    Set { cache_entry: CacheEntry, ttl_sender: TtlSchedulerManager },
    Save { outbox: mpsc::Sender<SaveCommand> },
    Get { key: String, sender: oneshot::Sender<QueryIO> },
    Keys { pattern: Option<String>, sender: oneshot::Sender<QueryIO> },
    Delete(String),
    StopSentinel,
}
