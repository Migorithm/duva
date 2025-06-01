use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::QueryIO;
use crate::domains::caches::cache_db::CacheDb;
use crate::domains::caches::read_queue::ReadQueue;
use crate::make_smart_pointer;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;

pub struct CacheActor {
    pub(crate) cache: CacheDb,
    pub(crate) self_handler: CacheCommandSender,
}

impl CacheActor {
    pub(crate) fn run(hwm: Arc<AtomicU64>) -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(
            Self {
                cache: CacheDb::with_capacity(1000),
                self_handler: CacheCommandSender(tx.clone()),
            }
            .handle(cache_actor_inbox, ReadQueue::new(hwm)),
        );
        CacheCommandSender(tx)
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }
    pub(crate) fn keys_with_expiry(&self) -> usize {
        self.cache.keys_with_expiry()
    }

    pub(crate) fn keys(&self, pattern: Option<String>, callback: oneshot::Sender<QueryIO>) {
        let keys = self
            .cache
            .keys()
            .filter_map(move |k| {
                if pattern.as_ref().is_none_or(|p| k.contains(p)) {
                    Some(QueryIO::BulkString(k.clone()))
                } else {
                    None
                }
            })
            .collect();
        let _ = callback.send(QueryIO::Array(keys));
    }
    pub(crate) fn delete(&mut self, key: String, callback: oneshot::Sender<bool>) {
        if let Some(_value) = self.cache.remove(&key) {
            let _ = callback.send(true);
        } else {
            let _ = callback.send(false);
        }
    }
    pub(crate) fn exists(&self, key: String, callback: oneshot::Sender<bool>) {
        let _ = callback.send(self.cache.get(&key).is_some());
    }
    pub(crate) fn get(&self, key: &str, callback: oneshot::Sender<Option<CacheValue>>) {
        let _ = callback.send(self.cache.get(key).cloned());
    }

    pub(crate) fn set(&mut self, cache_entry: CacheEntry) {
        let (key, value) = cache_entry.destructure();
        self.cache.insert(key, value);
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

    pub(crate) fn append(
        &mut self,
        key: String,
        value: String,
        callback: oneshot::Sender<anyhow::Result<usize>>,
    ) {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert(CacheValue { value: "".to_string(), expiry: None });
        val.value.push_str(value.as_str());

        let _ = callback.send(Ok(val.value.len()));
    }

    pub(crate) fn numeric_delta(
        &mut self,
        key: String,
        delta: i64,
        callback: oneshot::Sender<anyhow::Result<i64>>,
    ) {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert(CacheValue { value: "0".to_string(), expiry: None });

        let Ok(curr) = val.value.parse::<i64>() else {
            let _ =
                callback.send(Err(anyhow::anyhow!("ERR value is not an integer or out of range")));
            return;
        };

        let _ = callback.send(Ok(curr + delta));
        val.value = (curr + delta).to_string();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
