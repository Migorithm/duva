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
use tokio::sync::mpsc::{self, Receiver};

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

    async fn handle(mut self, mut recv: Receiver<CacheCommand>) -> anyhow::Result<Self> {
        while let Some(command) = recv.recv().await {
            match command {
                CacheCommand::Set { cache_entry } => {
                    self.set(cache_entry).await;
                },
                CacheCommand::Get { key, callback } => {
                    self.get(&key, callback);
                },
                CacheCommand::IndexGet { key, read_idx, callback } => {
                    self.index_get(&key, read_idx, callback);
                },
                CacheCommand::Keys { pattern, callback } => {
                    self.keys(pattern, callback);
                },
                CacheCommand::Delete { key, callback } => {
                    self.delete(key, callback);
                },
                CacheCommand::Exists { key, callback } => {
                    self.exists(key, callback);
                },
                CacheCommand::Save { outbox } => {
                    self.save(outbox).await?;
                },
                CacheCommand::Ping => {
                    self.flushout_readqueue();
                },
                CacheCommand::Drop { callback } => {
                    self.cache.clear();
                    callback.send(());
                },
                CacheCommand::Append { key, value, callback } => {
                    callback.send(self.append(key, value));
                },
                CacheCommand::NumericDetla { key, delta, callback } => {
                    callback.send(self.numeric_delta(key, delta));
                },
                CacheCommand::LPush { key, values, callback } => {
                    callback.send(self.lpush(key, values));
                },
                CacheCommand::LPushX { key, values, callback } => {
                    callback.send(self.lpushx(key, values));
                },
                CacheCommand::LPop { key, count, callback } => {
                    callback.send(self.pop(key, count, true));
                },
                CacheCommand::RPush { key, values, callback } => {
                    callback.send(self.rpush(key, values));
                },
                CacheCommand::RPushX { key, values, callback } => {
                    callback.send(self.rpushx(key, values));
                },
                CacheCommand::RPop { key, count, callback } => {
                    callback.send(self.pop(key, count, false));
                },
                CacheCommand::LLen { key, callback } => {
                    callback.send(self.llen(key));
                },
                CacheCommand::LRange { key, start, end, callback } => {
                    callback.send(self.lrange(key, start, end));
                },
                CacheCommand::LTrim { key, start, end, callback } => {
                    callback.send(self.ltrim(key, start, end));
                },
                CacheCommand::LIndex { key, index, callback } => {
                    callback.send(self.lindex(key, index));
                },
                CacheCommand::LSet { key, index, value, callback } => {
                    callback.send(self.lset(key, index, value));
                },
            }
        }
        Ok(self)
    }

    fn len(&self) -> usize {
        self.cache.len()
    }
    fn keys_with_expiry(&self) -> usize {
        self.cache.keys_with_expiry
    }

    fn keys(&self, pattern: Option<String>, callback: Callback<Vec<String>>) {
        let keys = self
            .cache
            .keys()
            .filter_map(move |k| {
                if pattern.as_ref().is_none_or(|p| k.contains(p)) { Some(k.clone()) } else { None }
            })
            .collect();
        callback.send(keys);
    }
    fn delete(&mut self, key: String, callback: Callback<bool>) {
        if let Some(_value) = self.cache.remove(&key) {
            callback.send(true);
        } else {
            callback.send(false);
        }
    }
    fn exists(&mut self, key: String, callback: Callback<bool>) {
        callback.send(self.cache.get(&key).is_some());
    }
    fn get(&mut self, key: &str, callback: Callback<CacheValue>) {
        callback.send(self.cache.get(key).cloned().unwrap_or(Default::default()));
    }

    fn index_get(&mut self, key: &str, read_idx: u64, callback: Callback<CacheValue>) {
        if let Some(callback) = self.read_queue.defer_if_stale(read_idx, key, callback) {
            self.get(key, callback);
        }
    }

    async fn set(&mut self, cache_entry: CacheEntry) {
        let _ = self.try_send_ttl(&cache_entry).await;
        let (key, value) = cache_entry.destructure();
        self.cache.put(key, value);
    }

    async fn try_send_ttl(&self, cache_entry: &CacheEntry) -> anyhow::Result<()> {
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

    fn append(&mut self, key: String, value: String) -> anyhow::Result<usize> {
        let val = self.cache.entry(key.clone()).or_insert(CacheValue::new(""));

        let mut current_str = val.try_to_string()?;
        current_str.push_str(value.as_str());
        val.value = TypedValue::String(Bytes::from(current_str).into());

        Ok(val.len())
    }

    fn numeric_delta(&mut self, key: String, delta: i64) -> anyhow::Result<i64> {
        let val = self.cache.entry(key.clone()).or_insert(CacheValue::new("0"));

        let curr = val
            .try_to_string()?
            .parse::<i64>()
            .context("ERR value is not an integer or out of range")?;

        val.value = TypedValue::String(Bytes::from((curr + delta).to_string()).into());
        Ok(curr + delta)
    }

    fn lpush(&mut self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
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
    fn lpushx(&mut self, key: String, values: Vec<String>) -> usize {
        let mut val = self.cache.get_mut(&key);

        let Some(CacheValue { value: TypedValue::List(list), .. }) = val.as_mut() else {
            return 0;
        };
        for v in values {
            list.lpush(v.into());
        }

        list.llen()
    }

    fn rpush(&mut self, key: String, values: Vec<String>) -> anyhow::Result<usize> {
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
    fn rpushx(&mut self, key: String, values: Vec<String>) -> usize {
        let mut val = self.cache.get_mut(&key);

        let Some(CacheValue { value: TypedValue::List(list), .. }) = val.as_mut() else {
            return 0;
        };
        for v in values {
            list.rpush(v.into());
        }

        list.llen()
    }

    fn pop(&mut self, key: String, count: usize, from_left: bool) -> Vec<String> {
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

    fn llen(&mut self, key: String) -> anyhow::Result<usize> {
        let Some(CacheValue { value, .. }) = self.cache.get(&key) else {
            return Ok(0);
        };
        match value {
            TypedValue::List(list) => Ok(list.llen()),
            _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    fn lrange(
        &mut self,
        key: String,
        start: isize,
        end: isize,
    ) -> Result<Vec<String>, anyhow::Error> {
        let Some(CacheValue { value, .. }) = self.cache.get_mut(&key) else {
            return Ok(vec![]);
        };
        match value {
            TypedValue::List(list) => Ok(list
                .lrange(start, end)
                .into_iter()
                .flat_map(|v| String::from_utf8(v.to_vec()))
                .collect()),
            _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    fn ltrim(&mut self, key: String, start: isize, end: isize) -> Result<(), anyhow::Error> {
        let Some(CacheValue { value, .. }) = self.cache.get_mut(&key) else {
            return Ok(());
        };
        match value {
            TypedValue::List(list) => {
                list.ltrim(start, end);
                Ok(())
            },
            _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    fn lindex(&mut self, key: String, index: isize) -> anyhow::Result<CacheValue> {
        let Some(CacheValue { value, .. }) = self.cache.get_mut(&key) else {
            return Ok(CacheValue::new(TypedValue::Null));
        };
        match value {
            TypedValue::List(list) => Ok(list
                .lindex(index)
                .map(|v| CacheValue::new(TypedValue::String(v.into())))
                .unwrap_or_default()),

            _ => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }

    fn lset(&mut self, key: String, index: isize, val: String) -> Result<(), anyhow::Error> {
        let Some(var) = self.cache.get_mut(&key) else {
            return Err(anyhow::anyhow!("ERR no such key"));
        };
        let CacheValue { value: TypedValue::List(list), .. } = var else {
            return Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG));
        };

        list.lset(index, val)
    }

    fn flushout_readqueue(&mut self) {
        if let Some(pending_rqs) = self.read_queue.take_pending_requests() {
            for DeferredRead { key, callback } in pending_rqs {
                self.get(&key, callback);
            }
        };
    }

    async fn save(
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

#[cfg(test)]
mod test {
    use crate::domains::caches::actor::CacheActor;
    use crate::domains::caches::actor::CacheCommandSender;

    use crate::domains::caches::cache_objects::CacheEntry;
    use crate::domains::caches::cache_objects::CacheValue;
    use crate::domains::caches::cache_objects::TypedValue;
    use crate::domains::caches::command::CacheCommand;
    use crate::domains::caches::lru_cache::LruCache;
    use crate::domains::caches::read_queue::ReadQueue;
    use crate::types::Callback;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;

    use tokio::time::timeout;

    struct S(Sender<CacheCommand>);
    impl S {
        async fn set(&self, key: String, value: &str) {
            self.0
                .send(CacheCommand::Set { cache_entry: CacheEntry::new(key, value) })
                .await
                .unwrap();
        }
        async fn get(&self, key: String, callback: Callback<CacheValue>) {
            self.0.send(CacheCommand::Get { key, callback }).await.unwrap();
        }
        async fn index_get(&self, key: String, read_idx: u64, callback: Callback<CacheValue>) {
            self.0.send(CacheCommand::IndexGet { key, read_idx, callback }).await.unwrap();
        }
        async fn ping(&self) {
            self.0.send(CacheCommand::Ping).await.unwrap();
        }
        async fn drop(&self) {
            let (tx, rx) = Callback::create();
            self.0.send(CacheCommand::Drop { callback: tx.into() }).await.unwrap();
            let _ = rx.recv().await;
        }
    }

    #[tokio::test]
    async fn test_index_get_put_in_rq() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let con_idx: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(cache.clone()),
                read_queue: ReadQueue::new(con_idx.clone()),
            }
            .handle(rx),
        );
        // WHEN
        let cache = S(cache);

        let key = "key".to_string();
        let value = "value";
        let (tx1, rx1) = Callback::create();
        let (tx2, rx2) = Callback::create();

        cache.set(key.clone(), value).await;
        cache.index_get(key.clone(), 0, tx1.into()).await;
        cache.index_get(key.clone(), 1, tx2.into()).await;

        // THEN
        let res1 = tokio::spawn(rx1.recv());
        let res2 = tokio::spawn(rx2.recv());

        assert_eq!(res1.await.unwrap(), CacheValue::new(value));

        let timeout = timeout(Duration::from_millis(1000), res2);
        assert!(timeout.await.is_err());
    }

    #[tokio::test]
    async fn test_index_get_returns_successfully_when_ping_is_made_after_con_idx_update() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let con_idx: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(cache.clone()),
                read_queue: ReadQueue::new(con_idx.clone()),
            }
            .handle(rx),
        );

        let cache = S(cache);

        let key = "key".to_string();
        let value = "value";
        cache.set(key.clone(), value).await;

        // ! Fail when con_idx wasn't updated and ping was not sent
        let (fail_t, fail_r) = Callback::create();
        cache.index_get(key.clone(), 1, fail_t.into()).await;
        timeout(Duration::from_millis(1000), fail_r.recv()).await.unwrap_err();

        // * success when con_idx was updated and ping was sent
        let (t, r) = Callback::create();
        cache.index_get(key.clone(), 1, t.into()).await;

        let task = tokio::spawn(r.recv());
        con_idx.store(1, std::sync::atomic::Ordering::Relaxed);
        cache.ping().await;

        // THEN
        assert_eq!(task.await.unwrap(), CacheValue::new(value));
    }

    #[tokio::test]
    async fn test_drop_cache() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let con_idx: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(cache.clone()),
                read_queue: ReadQueue::new(con_idx.clone()),
            }
            .handle(rx),
        );
        // WHEN
        let cache = S(cache);

        cache.set("key".to_string().clone(), "value").await;
        cache.set("key1".to_string().clone(), "value1").await;
        cache.drop().await;

        // THEN
        let (tx, rx) = Callback::create();
        cache.get("key".to_string().clone(), tx.into()).await;

        assert!(matches!(rx.recv().await, CacheValue { value: TypedValue::Null, .. }));

        let (tx, rx) = Callback::create();
        cache.get("key1".to_string().clone(), tx.into()).await;

        assert!(matches!(rx.recv().await, CacheValue { value: TypedValue::Null, .. }));
    }
}
