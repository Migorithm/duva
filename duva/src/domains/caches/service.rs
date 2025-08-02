use crate::domains::caches::actor::CacheActor;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::caches::command::CacheCommand;
use crate::domains::caches::read_queue::{DeferredRead, ReadQueue};

use crate::domains::saves::command::SaveCommand;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;

impl CacheActor {
    pub(super) async fn handle(
        mut self,
        mut recv: Receiver<CacheCommand>,
        mut rq: ReadQueue,
    ) -> Result<Self> {
        while let Some(command) = recv.recv().await {
            match command {
                | CacheCommand::Set { cache_entry } => {
                    let _ = self.try_send_ttl(&cache_entry).await;
                    self.set(cache_entry);
                },
                | CacheCommand::Get { key, callback } => {
                    self.get(&key, callback);
                },
                | CacheCommand::IndexGet { key, read_idx, callback } => {
                    if let Some(callback) = rq.defer_if_stale(read_idx, &key, callback) {
                        self.get(&key, callback);
                    }
                },
                | CacheCommand::Keys { pattern, callback } => {
                    self.keys(pattern, callback);
                },
                | CacheCommand::Delete { key, callback } => {
                    self.delete(key, callback);
                },
                | CacheCommand::Exists { key, callback } => {
                    self.exists(key, callback);
                },
                | CacheCommand::Save { outbox } => {
                    outbox
                        .send(SaveCommand::LocalShardSize {
                            table_size: self.len(),
                            expiry_size: self.keys_with_expiry(),
                        })
                        .await?;

                    for chunk in self.cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox.send(SaveCommand::SaveChunk(CacheEntry::from_slice(chunk))).await?;
                    }
                    // finalize the save operation
                    outbox.send(SaveCommand::StopSentinel).await?;
                },
                | CacheCommand::Ping => {
                    if let Some(pending_rqs) = rq.take_pending_requests() {
                        for DeferredRead { key, callback } in pending_rqs {
                            self.get(&key, callback);
                        }
                    };
                },
                | CacheCommand::Drop { callback } => {
                    self.cache.clear();
                    callback.send(());
                },
                | CacheCommand::Append { key, value, callback } => {
                    callback.send(self.append(key, value));
                },
                | CacheCommand::NumericDetla { key, delta, callback } => {
                    callback.send(self.numeric_delta(key, delta));
                },
                | CacheCommand::LPush { key, values, callback } => {
                    callback.send(self.lpush(key, values));
                },
                | CacheCommand::LPushX { key, values, callback } => {
                    callback.send(self.lpushx(key, values));
                },
                | CacheCommand::LPop { key, count, callback } => {
                    callback.send(self.pop(key, count, true));
                },
                | CacheCommand::RPush { key, values, callback } => {
                    callback.send(self.rpush(key, values));
                },
                | CacheCommand::RPushX { key, values, callback } => {
                    callback.send(self.rpushx(key, values));
                },
                | CacheCommand::RPop { key, count, callback } => {
                    callback.send(self.pop(key, count, false));
                },
                | CacheCommand::LLen { key, callback } => {
                    callback.send(self.llen(key));
                },
                | CacheCommand::LRange { key, start, end, callback } => {
                    callback.send(self.lrange(key, start, end));
                },
                | CacheCommand::LTrim { key, start, end, callback } => {
                    callback.send(self.ltrim(key, start, end));
                },
                | CacheCommand::LIndex { key, index, callback } => {
                    callback.send(self.lindex(key, index));
                },
                | CacheCommand::LSet { key, index, value, callback } => {
                    callback.send(self.lset(key, index, value));
                },
            }
        }
        Ok(self)
    }
}

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
            let _ = rx.await;
        }
    }

    #[tokio::test]
    async fn test_index_get_put_in_rq() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let hwm: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(cache.clone()),
            }
            .handle(rx, ReadQueue::new(hwm.clone())),
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
        let res1 = tokio::spawn(rx1);
        let res2 = tokio::spawn(rx2);

        assert_eq!(res1.await.unwrap().unwrap(), CacheValue::new(value));

        let timeout = timeout(Duration::from_millis(1000), res2);
        assert!(timeout.await.is_err());
    }

    #[tokio::test]
    async fn test_index_get_returns_successfully_when_ping_is_made_after_hwm_update() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let hwm: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(cache.clone()),
            }
            .handle(rx, ReadQueue::new(hwm.clone())),
        );

        let cache = S(cache);

        let key = "key".to_string();
        let value = "value";
        cache.set(key.clone(), value).await;

        // ! Fail when hwm wasn't updated and ping was not sent
        let (fail_t, fail_r) = Callback::create();
        cache.index_get(key.clone(), 1, fail_t.into()).await;
        timeout(Duration::from_millis(1000), fail_r).await.unwrap_err();

        // * success when hwm was updated and ping was sent
        let (t, r) = Callback::create();
        cache.index_get(key.clone(), 1, t.into()).await;

        let task = tokio::spawn(r);
        hwm.store(1, std::sync::atomic::Ordering::Relaxed);
        cache.ping().await;

        // THEN
        assert_eq!(task.await.unwrap().unwrap(), CacheValue::new(value));
    }

    #[tokio::test]
    async fn test_drop_cache() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let hwm: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor {
                cache: LruCache::new(1000),
                self_handler: CacheCommandSender(cache.clone()),
            }
            .handle(rx, ReadQueue::new(hwm.clone())),
        );
        // WHEN
        let cache = S(cache);

        cache.set("key".to_string().clone(), "value").await;
        cache.set("key1".to_string().clone(), "value1").await;
        cache.drop().await;

        // THEN
        let (tx, rx) = Callback::create();
        cache.get("key".to_string().clone(), tx.into()).await;

        assert!(matches!(rx.await, Ok(CacheValue { value: TypedValue::Null, .. })));

        let (tx, rx) = Callback::create();
        cache.get("key1".to_string().clone(), tx.into()).await;

        assert!(matches!(rx.await, Ok(CacheValue { value: TypedValue::Null, .. })));
    }
}
