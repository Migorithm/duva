use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::caches::cache_objects::TypedValue;
use crate::domains::caches::cache_objects::value::WRONG_TYPE_ERR_MSG;
use crate::domains::caches::lru_cache::LruCache;
use crate::domains::caches::read_queue::ReadQueue;
use crate::make_smart_pointer;
use crate::types::Callback;
use anyhow::Context;
use bytes::Bytes;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{self};

pub struct CacheActor {
    pub(crate) segments: Vec<RwLock<LruCache<String, CacheValue>>>,
    pub(crate) self_handler: CacheCommandSender,
}

impl CacheActor {
    pub(crate) fn run(con_idx: Arc<AtomicU64>) -> CacheCommandSender {
        const NUM_SEGMENTS: usize = 32;
        let (tx, cache_actor_inbox) = mpsc::channel(2000);
        let segments =
            (0..NUM_SEGMENTS).map(|_| RwLock::new(LruCache::new(10000 / NUM_SEGMENTS))).collect();
        tokio::spawn(
            Self { segments, self_handler: CacheCommandSender(tx.clone()) }
                .handle(cache_actor_inbox, ReadQueue::new(con_idx)),
        );
        CacheCommandSender(tx)
    }

    fn select_segment(&self, key: &str) -> &RwLock<LruCache<String, CacheValue>> {
        let mut hasher = std::hash::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        &self.segments[hash % self.segments.len()]
    }

    pub(crate) async fn len(&self) -> usize {
        let mut total = 0;
        for segment in &self.segments {
            total += segment.read().await.len();
        }
        total
    }

    pub(crate) async fn keys_with_expiry(&self) -> usize {
        let mut total = 0;
        for segment in &self.segments {
            total += segment.read().await.keys_with_expiry;
        }
        total
    }

    pub(crate) async fn keys(&self, pattern: Option<String>, callback: Callback<Vec<String>>) {
        let mut all_keys = Vec::new();
        for segment in &self.segments {
            let guard = segment.read().await;
            let keys: Vec<String> = guard
                .keys()
                .filter_map(|k| {
                    if pattern.as_ref().is_none_or(|p| k.contains(p)) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();
            all_keys.extend(keys);
        }
        callback.send(all_keys);
    }
    pub(crate) async fn delete(&self, key: String, callback: Callback<bool>) {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        if let Some(_value) = guard.remove(&key) {
            callback.send(true);
        } else {
            callback.send(false);
        }
    }
    pub(crate) async fn exists(&self, key: String, callback: Callback<bool>) {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        callback.send(guard.get(&key).is_some());
    }
    pub(crate) async fn get(&self, key: &str, callback: Callback<CacheValue>) {
        let segment = self.select_segment(key);
        let mut guard = segment.write().await;
        callback.send(guard.get(key).cloned().unwrap_or(Default::default()));
    }

    pub(crate) async fn set(&self, cache_entry: CacheEntry) {
        let (key, value) = cache_entry.destructure();
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        guard.put(key, value);
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

    pub(crate) async fn append(&self, key: String, value: String) -> anyhow::Result<usize> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard.entry(key.clone()).or_insert(CacheValue::new(""));

        let mut current_str = val.try_to_string()?;
        current_str.push_str(value.as_str());
        val.value = TypedValue::String(Bytes::from(current_str).into());

        Ok(val.len())
    }

    pub(crate) async fn numeric_delta(&self, key: String, delta: i64) -> anyhow::Result<i64> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard.entry(key.clone()).or_insert(CacheValue::new("0"));

        let curr = val
            .try_to_string()?
            .parse::<i64>()
            .context("ERR value is not an integer or out of range")?;

        val.value = TypedValue::String(Bytes::from((curr + delta).to_string()).into());
        Ok(curr + delta)
    }

    pub(crate) async fn lpush(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard
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
    pub(crate) async fn lpushx(&self, key: String, values: Vec<String>) -> usize {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard.get_mut(&key);

        let Some(CacheValue { value: TypedValue::List(list), .. }) = val else {
            return 0;
        };
        for v in values {
            list.lpush(v.into());
        }

        list.llen()
    }

    pub(crate) async fn rpush(&self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard
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
    pub(crate) async fn rpushx(&self, key: String, values: Vec<String>) -> usize {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard.get_mut(&key);

        let Some(CacheValue { value: TypedValue::List(list), .. }) = val else {
            return 0;
        };
        for v in values {
            list.rpush(v.into());
        }

        list.llen()
    }

    pub(crate) async fn pop(&self, key: String, count: usize, from_left: bool) -> Vec<String> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let val = guard.remove(&key);
        if let Some(CacheValue { value: TypedValue::List(mut list), .. }) = val {
            let vals = (0..count)
                .filter_map(|_| if from_left { list.lpop() } else { list.rpop() }) // Convert to Iterator<Item = Bytes>
                .flat_map(|v| String::from_utf8(v.to_vec())) // Convert to Iterator<Item= Result<String>>
                .collect();
            if list.llen() != 0 {
                guard.put(key, CacheValue::new(TypedValue::List(list)));
            }
            vals
        } else {
            vec![]
        }
    }

    pub(crate) async fn llen(&self, key: String) -> anyhow::Result<usize> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let Some(CacheValue { value, .. }) = guard.get(&key) else {
            return Ok(0);
        };
        match value {
            | TypedValue::List(list) => Ok(list.llen()),
            | _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    pub(crate) async fn lrange(
        &self,
        key: String,
        start: isize,
        end: isize,
    ) -> Result<Vec<String>, anyhow::Error> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let Some(CacheValue { value, .. }) = guard.get_mut(&key) else {
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

    pub(crate) async fn ltrim(
        &self,
        key: String,
        start: isize,
        end: isize,
    ) -> Result<(), anyhow::Error> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let Some(CacheValue { value, .. }) = guard.get_mut(&key) else {
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

    pub(crate) async fn lindex(&self, key: String, index: isize) -> anyhow::Result<CacheValue> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let Some(CacheValue { value, .. }) = guard.get_mut(&key) else {
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

    pub(crate) async fn lset(
        &self,
        key: String,
        index: isize,
        val: String,
    ) -> Result<(), anyhow::Error> {
        let segment = self.select_segment(&key);
        let mut guard = segment.write().await;
        let Some(var) = guard.get_mut(&key) else {
            return Err(anyhow::anyhow!("ERR no such key"));
        };
        let CacheValue { value: TypedValue::List(list), .. } = var else {
            return Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG));
        };

        list.lset(index, val)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
