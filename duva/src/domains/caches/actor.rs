use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::caches::cache_objects::TypedValue;
use crate::domains::caches::cache_objects::value::WRONG_TYPE_ERR_MSG;
use crate::domains::caches::lru_cache::LruCache;
use crate::domains::caches::read_queue::{DeferredRead, ReadQueue};
use crate::domains::saves::command::SaveCommand;
use crate::make_smart_pointer;
use crate::types::Callback;
use anyhow::Context;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc::{self};

pub struct CacheActor {
    pub(crate) cache: LruCache<String, CacheValue>,
    pub(crate) self_handler: CacheCommandSender,
    pub(crate) read_queue: ReadQueue,
}

impl CacheActor {
    pub(crate) fn run(con_idx: Arc<AtomicU64>) -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(2000);
        tokio::spawn(
            Self {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(tx.clone()),
                read_queue: ReadQueue::new(con_idx),
            }
            .handle(cache_actor_inbox),
        );
        CacheCommandSender(tx)
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }
    pub(crate) fn keys_with_expiry(&self) -> usize {
        self.cache.keys_with_expiry
    }

    pub(crate) fn keys(&self, pattern: Option<String>, callback: Callback<Vec<String>>) {
        let keys = self
            .cache
            .keys()
            .filter_map(move |k| {
                if pattern.as_ref().is_none_or(|p| k.contains(p)) { Some(k.clone()) } else { None }
            })
            .collect();
        callback.send(keys);
    }
    pub(crate) fn delete(&mut self, key: String, callback: Callback<bool>) {
        if let Some(_value) = self.cache.remove(&key) {
            callback.send(true);
        } else {
            callback.send(false);
        }
    }
    pub(crate) fn exists(&mut self, key: String, callback: Callback<bool>) {
        callback.send(self.cache.get(&key).is_some());
    }
    pub(crate) fn get(&mut self, key: &str, callback: Callback<CacheValue>) {
        callback.send(self.cache.get(key).cloned().unwrap_or(Default::default()));
    }

    pub(crate) fn index_get(&mut self, key: &str, read_idx: u64, callback: Callback<CacheValue>) {
        if let Some(callback) = self.read_queue.defer_if_stale(read_idx, key, callback) {
            self.get(key, callback);
        }
    }

    pub(crate) async fn set(&mut self, cache_entry: CacheEntry) {
        let _ = self.try_send_ttl(&cache_entry).await;
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
                let (callback, rx) = Callback::create();
                let _ = handler.send(CacheCommand::Delete { key, callback }).await;
                let _ = rx.recv().await;
            }
        });
        Ok(())
    }

    pub(crate) fn append(&mut self, key: String, value: String) -> anyhow::Result<usize> {
        let val = self.cache.entry(key.clone()).or_insert(CacheValue::new(""));

        let mut current_str = val.try_to_string()?;
        current_str.push_str(value.as_str());
        val.value = TypedValue::String(Bytes::from(current_str).into());

        Ok(val.len())
    }

    pub(crate) fn numeric_delta(&mut self, key: String, delta: i64) -> anyhow::Result<i64> {
        let val = self.cache.entry(key.clone()).or_insert(CacheValue::new("0"));

        let curr = val
            .try_to_string()?
            .parse::<i64>()
            .context("ERR value is not an integer or out of range")?;

        val.value = TypedValue::String(Bytes::from((curr + delta).to_string()).into());
        Ok(curr + delta)
    }

    pub(crate) fn lpush(&mut self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert(CacheValue::new(TypedValue::List(Default::default())));

        let TypedValue::List(ref mut list) = val.value else {
            return Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG));
        };
        for v in values {
            list.lpush(v.into());
        }

        Ok(list.llen())
    }
    pub(crate) fn lpushx(&mut self, key: String, values: Vec<String>) -> usize {
        let mut val = self.cache.get_mut(&key);

        let Some(CacheValue { value: TypedValue::List(list), .. }) = val.as_mut() else {
            return 0;
        };
        for v in values {
            list.lpush(v.into());
        }

        list.llen()
    }

    pub(crate) fn rpush(&mut self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert(CacheValue::new(TypedValue::List(Default::default())));

        let TypedValue::List(ref mut list) = val.value else {
            return Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG));
        };
        for v in values {
            list.rpush(v.into());
        }

        Ok(list.llen())
    }
    pub(crate) fn rpushx(&mut self, key: String, values: Vec<String>) -> usize {
        let mut val = self.cache.get_mut(&key);

        let Some(CacheValue { value: TypedValue::List(list), .. }) = val.as_mut() else {
            return 0;
        };
        for v in values {
            list.rpush(v.into());
        }

        list.llen()
    }

    pub(crate) fn pop(&mut self, key: String, count: usize, from_left: bool) -> Vec<String> {
        let val = self.cache.remove(&key);
        if let Some(CacheValue { value: TypedValue::List(mut list), .. }) = val {
            let vals = (0..count)
                .filter_map(|_| if from_left { list.lpop() } else { list.rpop() }) // Convert to Iterator<Item = Bytes>
                .flat_map(|v| String::from_utf8(v.to_vec())) // Convert to Iterator<Item= Result<String>>
                .collect();
            if list.llen() != 0 {
                self.cache.put(key, CacheValue::new(TypedValue::List(list)));
            }
            vals
        } else {
            vec![]
        }
    }

    pub(crate) fn llen(&mut self, key: String) -> anyhow::Result<usize> {
        let Some(CacheValue { value, .. }) = self.cache.get(&key) else {
            return Ok(0);
        };
        match value {
            | TypedValue::List(list) => Ok(list.llen()),
            | _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    pub(crate) fn lrange(
        &mut self,
        key: String,
        start: isize,
        end: isize,
    ) -> Result<Vec<String>, anyhow::Error> {
        let Some(CacheValue { value, .. }) = self.cache.get_mut(&key) else {
            return Ok(vec![]);
        };
        match value {
            | TypedValue::List(list) => Ok(list
                .lrange(start, end)
                .into_iter()
                .flat_map(|v| String::from_utf8(v.to_vec()))
                .collect()),
            | _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    pub(crate) fn ltrim(
        &mut self,
        key: String,
        start: isize,
        end: isize,
    ) -> Result<(), anyhow::Error> {
        let Some(CacheValue { value, .. }) = self.cache.get_mut(&key) else {
            return Ok(());
        };
        match value {
            | TypedValue::List(list) => {
                list.ltrim(start, end);
                Ok(())
            },
            | _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    pub(crate) fn lindex(&mut self, key: String, index: isize) -> anyhow::Result<CacheValue> {
        let Some(CacheValue { value, .. }) = self.cache.get_mut(&key) else {
            return Ok(CacheValue::new(TypedValue::Null));
        };
        match value {
            | TypedValue::List(list) => Ok(list
                .lindex(index)
                .map(|v| CacheValue::new(TypedValue::String(v.into())))
                .unwrap_or_default()),

            | _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    pub(crate) fn lset(
        &mut self,
        key: String,
        index: isize,
        val: String,
    ) -> Result<(), anyhow::Error> {
        let Some(var) = self.cache.get_mut(&key) else {
            return Err(anyhow::anyhow!("ERR no such key"));
        };
        let CacheValue { value: TypedValue::List(list), .. } = var else {
            return Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG));
        };

        list.lset(index, val)
    }

    pub(crate) fn flushout_readqueue(&mut self) {
        if let Some(pending_rqs) = self.read_queue.take_pending_requests() {
            for DeferredRead { key, callback } in pending_rqs {
                self.get(&key, callback);
            }
        };
    }

    pub(crate) async fn save(
        &self,
        outbox: tokio::sync::mpsc::Sender<SaveCommand>,
    ) -> Result<(), anyhow::Error> {
        outbox
            .send(SaveCommand::LocalShardSize {
                table_size: self.len(),
                expiry_size: self.keys_with_expiry(),
            })
            .await?;
        for chunk in self.cache.iter().collect::<Vec<_>>().chunks(10) {
            outbox.send(SaveCommand::SaveChunk(CacheEntry::from_slice(chunk))).await?;
        }
        Ok(outbox.send(SaveCommand::StopSentinel).await?)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
