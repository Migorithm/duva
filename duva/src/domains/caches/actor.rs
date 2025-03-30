use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::caches::read_queue::ReadQueue;
use crate::domains::query_parsers::QueryIO;
use crate::make_smart_pointer;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;

pub struct CacheActor {
    pub(crate) cache: CacheDb,
    pub(crate) self_handler: Sender<CacheCommand>,
}

#[derive(Default)]
pub(crate) struct CacheDb {
    inner: HashMap<String, CacheValue>,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    pub(crate) keys_with_expiry: usize,
}

impl CacheActor {
    pub(crate) fn run(hwm: Arc<AtomicU64>) -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(
            Self { cache: CacheDb::default(), self_handler: tx.clone() }
                .handle(cache_actor_inbox, ReadQueue::new(hwm)),
        );
        CacheCommandSender(tx)
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }
    pub(crate) fn keys_with_expiry(&self) -> usize {
        self.cache.keys_with_expiry
    }

    pub(crate) fn keys_stream(
        &self,
        pattern: Option<String>,
    ) -> impl Iterator<Item = QueryIO> + '_ {
        self.cache.keys().filter_map(move |k| {
            if pattern.as_ref().is_none_or(|p| k.contains(p)) {
                Some(QueryIO::BulkString(k.clone()))
            } else {
                None
            }
        })
    }
    pub(crate) fn delete(&mut self, key: &str) {
        if let Some(value) = self.cache.remove(key) {
            if value.has_expiry() {
                self.cache.keys_with_expiry -= 1;
            }
        }
    }
    pub(crate) fn get(&self, key: &str, callback: oneshot::Sender<QueryIO>) {
        let _ = callback.send(self.cache.get(key).cloned().into());
    }

    pub(crate) fn set(&mut self, cache_entry: CacheEntry) {
        match cache_entry {
            CacheEntry::KeyValue(key, value) => {
                self.cache.insert(key, CacheValue::Value(value));
            },
            CacheEntry::KeyValueExpiry(key, value, expiry) => {
                self.cache.keys_with_expiry += 1;
                self.cache.insert(key.clone(), CacheValue::ValueWithExpiry(value, expiry));
            },
        }
    }

    pub(crate) async fn try_send_ttl(&self, cache_entry: &CacheEntry) -> anyhow::Result<()> {
        let Some(expire_in) = cache_entry.expire_in()? else { return Ok(()) };
        let handler = self.self_handler.clone();
        tokio::spawn({
            let key = cache_entry.key().to_string();
            async move {
                tokio::time::sleep(expire_in).await;
                let _ = handler.send(CacheCommand::Delete(key)).await;
            }
        });
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
make_smart_pointer!(CacheDb, HashMap<String, CacheValue> => inner);
