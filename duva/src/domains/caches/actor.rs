use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::caches::cache_objects::TypedValue;
use crate::domains::caches::lru_cache::LruCache;
use crate::domains::caches::read_queue::ReadQueue;
use crate::make_smart_pointer;
use anyhow::Context;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;

pub struct CacheActor {
    pub(crate) cache: LruCache<String, CacheValue>,
    pub(crate) self_handler: CacheCommandSender,
}

impl CacheActor {
    pub(crate) fn run(hwm: Arc<AtomicU64>) -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(
            Self { cache: LruCache::new(1000), self_handler: CacheCommandSender(tx.clone()) }
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

    pub(crate) fn keys(&self, pattern: Option<String>, callback: oneshot::Sender<Vec<String>>) {
        let keys = self
            .cache
            .keys()
            .filter_map(move |k| {
                if pattern.as_ref().is_none_or(|p| k.contains(p)) { Some(k.clone()) } else { None }
            })
            .collect();
        let _ = callback.send(keys);
    }
    pub(crate) fn delete(&mut self, key: String, callback: oneshot::Sender<bool>) {
        if let Some(_value) = self.cache.remove(&key) {
            let _ = callback.send(true);
        } else {
            let _ = callback.send(false);
        }
    }
    pub(crate) fn exists(&mut self, key: String, callback: oneshot::Sender<bool>) {
        let _ = callback.send(self.cache.get(&key).is_some());
    }
    pub(crate) fn get(&mut self, key: &str, callback: oneshot::Sender<CacheValue>) {
        let _ = callback.send(self.cache.get(key).cloned().unwrap_or(Default::default()));
    }

    pub(crate) fn set(&mut self, cache_entry: CacheEntry) {
        let (key, value) = cache_entry.destructure();
        self.cache.put(key, value);
    }

    pub(crate) async fn try_send_ttl(&self, cache_entry: &CacheEntry) -> anyhow::Result<()> {
        let Some(expire_in) = cache_entry.expire_in()? else { return Ok(()) };
        let handler = self.self_handler.clone();
        tokio::spawn({
            let key = cache_entry.key().to_string();
            async move {
                tokio::time::sleep(expire_in).await;
                let (tx, rx) = oneshot::channel();
                let _ = handler.send(CacheCommand::Delete { key, callback: tx }).await;
                let _ = rx.await;
            }
        });
        Ok(())
    }

    pub(crate) fn append(&mut self, key: String, value: String) -> anyhow::Result<usize> {
        let val = self.cache.entry(key.clone()).or_insert(CacheValue::new(""));

        let bytes = val.value.as_bytes()?;
        let mut current_str = String::from_utf8_lossy(bytes).to_string();
        current_str.push_str(value.as_str());
        val.value = TypedValue::String(Bytes::from(current_str));

        Ok(val.value.as_bytes()?.len())
    }

    pub(crate) fn numeric_delta(&mut self, key: String, delta: i64) -> anyhow::Result<i64> {
        let val = self.cache.entry(key.clone()).or_insert(CacheValue::new("0"));

        let curr = String::from_utf8_lossy(&val.value().as_bytes()?)
            .parse::<i64>()
            .context("ERR value is not an integer or out of range")?;

        val.value = TypedValue::String(Bytes::from((curr + delta).to_string()));
        Ok(curr + delta)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
