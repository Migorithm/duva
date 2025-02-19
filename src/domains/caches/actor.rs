use std::collections::HashMap;

use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::query_parsers::QueryIO;
use crate::domains::ttl::manager::TtlSchedulerManager;
use crate::make_smart_pointer;

use anyhow::Context;
use tokio::sync::mpsc;

pub struct CacheActor {
    pub(crate) inbox: mpsc::Receiver<CacheCommand>,
    pub(crate) cache: CacheDb,
}

#[derive(Default)]
pub struct CacheDb {
    inner: HashMap<String, CacheValue>,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    pub(crate) keys_with_expiry: usize,
}

impl CacheActor {
    pub fn run() -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(Self { inbox: cache_actor_inbox, cache: CacheDb::default() }.handle());
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
            if pattern.as_ref().map_or(true, |p| k.contains(p)) {
                Some(QueryIO::BulkString(k.clone().into()))
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
    pub(crate) fn get(&self, key: &str) -> Option<CacheValue> {
        self.cache.get(key).cloned()
    }

    pub(crate) fn set(&mut self, cache_entry: CacheEntry) {
        match cache_entry {
            CacheEntry::KeyValue(key, value) => {
                self.cache.insert(key, CacheValue::Value(value));
            }
            CacheEntry::KeyValueExpiry(key, value, expiry) => {
                self.cache.keys_with_expiry += 1;
                self.cache.insert(key.clone(), CacheValue::ValueWithExpiry(value, expiry));
            }
        }
    }

    pub(crate) async fn try_send_ttl(
        &self,
        key: &str,
        expiry: Option<std::time::SystemTime>,
        ttl_sender: TtlSchedulerManager,
    ) -> anyhow::Result<()> {
        ttl_sender.set_ttl(key.to_string(), expiry.context("Expiry not found")?).await;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CacheCommandSender(mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
make_smart_pointer!(CacheDb, HashMap<String, CacheValue> => inner);
