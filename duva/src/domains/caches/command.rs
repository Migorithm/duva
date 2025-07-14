use super::cache_objects::{CacheEntry, CacheValue};
use crate::{domains::saves::command::SaveCommand, types::Callback};
use tokio::sync::{mpsc, oneshot};

pub(crate) enum CacheCommand {
    Set {
        cache_entry: CacheEntry,
    },
    Save {
        outbox: mpsc::Sender<SaveCommand>,
    },
    Get {
        key: String,
        callback: oneshot::Sender<CacheValue>,
    },
    Keys {
        pattern: Option<String>,
        callback: oneshot::Sender<Vec<String>>,
    },
    Delete {
        key: String,
        callback: oneshot::Sender<bool>,
    },
    IndexGet {
        key: String,
        read_idx: u64,
        callback: oneshot::Sender<CacheValue>,
    },
    Ping,
    Drop {
        callback: oneshot::Sender<()>,
    },
    Exists {
        key: String,
        callback: oneshot::Sender<bool>,
    },
    Append {
        key: String,
        value: String,
        callback: oneshot::Sender<anyhow::Result<usize>>,
    },
    NumericDetla {
        key: String,
        delta: i64,
        callback: oneshot::Sender<anyhow::Result<i64>>,
    },
    LPush {
        key: String,
        values: Vec<String>,
        callback: Callback<anyhow::Result<usize>>,
    },
    LPushX {
        key: String,
        values: Vec<String>,
        callback: Callback<usize>,
    },
    RPush {
        key: String,
        values: Vec<String>,
        callback: Callback<anyhow::Result<usize>>,
    },
    LPop {
        key: String,
        count: usize,
        callback: Callback<Vec<String>>,
    },
    RPop {
        key: String,
        count: usize,
        callback: Callback<Vec<String>>,
    },
    LLen {
        key: String,
        callback: Callback<anyhow::Result<usize>>,
    },
    LRange {
        key: String,
        start: isize,
        end: isize,
        callback: Callback<anyhow::Result<Vec<String>>>,
    },
    LTrim {
        key: String,
        start: isize,
        end: isize,
        callback: Callback<anyhow::Result<()>>,
    },
    LIndex {
        key: String,
        index: isize,
        callback: Callback<anyhow::Result<CacheValue>>,
    },
}
