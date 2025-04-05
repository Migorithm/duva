use crate::domains::caches::actor::CacheActor;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::caches::command::CacheCommand;
use crate::domains::caches::read_queue::{DeferredRead, ReadQueue};
use crate::domains::query_parsers::QueryIO;
use crate::domains::saves::command::SaveCommand;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;

impl CacheActor {
    pub(crate) async fn handle(
        mut self,
        mut recv: Receiver<CacheCommand>,
        mut rq: ReadQueue,
    ) -> Result<Self> {
        while let Some(command) = recv.recv().await {
            match command {
                CacheCommand::StopSentinel => break,
                CacheCommand::Set { cache_entry } => {
                    let _ = self.try_send_ttl(&cache_entry).await;
                    self.set(cache_entry);
                },
                CacheCommand::Get { key, callback } => {
                    self.get(&key, callback);
                },
                CacheCommand::IndexGet { key, read_idx, callback } => {
                    if let Some(callback) = rq.defer_if_stale(read_idx, &key, callback) {
                        self.get(&key, callback);
                    }
                },
                CacheCommand::Keys { pattern, callback: sender } => {
                    let ks: Vec<_> = self.keys_stream(pattern).collect();

                    sender
                        .send(QueryIO::Array(ks))
                        .map_err(|_| anyhow::anyhow!("Error sending keys"))?;
                },
                CacheCommand::Delete { key, callback } => {
                    self.delete(key, callback);
                },
                CacheCommand::Save { outbox } => {
                    outbox
                        .send(SaveCommand::LocalShardSize {
                            table_size: self.len(),
                            expiry_size: self.keys_with_expiry(),
                        })
                        .await?;
                    for chunk in self.cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox.send(SaveCommand::SaveChunk(CacheEntry::new(chunk))).await?;
                    }
                    // finalize the save operation
                    outbox.send(SaveCommand::StopSentinel).await?;
                },
                CacheCommand::Ping => {
                    if let Some(pending_rqs) = rq.take_pending_requests() {
                        for DeferredRead { key, callback } in pending_rqs {
                            self.get(&key, callback);
                        }
                    };
                },
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod test {
    use crate::domains::caches::actor::CacheActor;
    use crate::domains::caches::actor::CacheDb;
    use crate::domains::caches::cache_objects::CacheEntry;
    use crate::domains::caches::command::CacheCommand;
    use crate::domains::caches::read_queue::ReadQueue;
    use crate::domains::query_parsers::QueryIO;
    use chrono::Utc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    struct S(Sender<CacheCommand>);
    impl S {
        async fn set(&self, key: String, value: String) {
            self.0
                .send(CacheCommand::Set { cache_entry: CacheEntry::KeyValue(key, value) })
                .await
                .unwrap();
        }
        async fn index_get(&self, key: String, read_idx: u64, callback: oneshot::Sender<QueryIO>) {
            self.0.send(CacheCommand::IndexGet { key, read_idx, callback }).await.unwrap();
        }
        async fn ping(&self) {
            self.0.send(CacheCommand::Ping).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_set_and_delete_inc_dec_keys_with_expiry() {
        // GIVEN
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let hwm = Arc::new(0.into());
        let actor = CacheActor { cache: CacheDb::default(), self_handler: tx.clone() };

        // WHEN

        let handler = tokio::spawn(actor.handle(rx, ReadQueue::new(hwm)));

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            tx.send(CacheCommand::Set {
                cache_entry: if i & 1 == 0 {
                    CacheEntry::KeyValueExpiry(key, value, Utc::now() + Duration::from_secs(10))
                } else {
                    CacheEntry::KeyValue(key, value)
                },
            })
            .await
            .unwrap();
        }

        // key0 is expiry key. deleting the following will decrese the number by 1
        let delete_key = "key0".to_string();
        let (callback, rx) = oneshot::channel();
        tx.send(CacheCommand::Delete { key: delete_key, callback }).await.unwrap();
        tx.send(CacheCommand::StopSentinel).await.unwrap();
        let actor: CacheActor = handler.await.unwrap().unwrap();
        let res = rx.await.unwrap();

        // THEN
        assert!(res);
        assert_eq!(actor.cache.keys_with_expiry, 49);
    }

    #[tokio::test]
    async fn test_index_get_put_in_rq() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let hwm: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor { cache: CacheDb::default(), self_handler: cache.clone() }
                .handle(rx, ReadQueue::new(hwm.clone())),
        );
        // WHEN
        let cache = S(cache);

        let key = "key".to_string();
        let value = "value".to_string();
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        cache.set(key.clone(), value.clone()).await;
        cache.index_get(key.clone(), 0, tx1).await;
        cache.index_get(key.clone(), 1, tx2).await;

        let expected_res = QueryIO::BulkString(value.clone().into());

        // THEN
        let res1 = tokio::spawn(rx1);
        let res2 = tokio::spawn(rx2);

        assert_eq!(res1.await.unwrap().unwrap(), expected_res.clone());

        let timeout = timeout(Duration::from_millis(1000), res2);
        assert!(timeout.await.is_err());
    }

    #[tokio::test]
    async fn test_index_get_returns_successfully_when_ping_is_made_after_hwm_update() {
        // GIVEN
        let (cache, rx) = tokio::sync::mpsc::channel(100);
        let hwm: Arc<AtomicU64> = Arc::new(0.into());
        tokio::spawn(
            CacheActor { cache: CacheDb::default(), self_handler: cache.clone() }
                .handle(rx, ReadQueue::new(hwm.clone())),
        );

        let cache = S(cache);

        let key = "key".to_string();
        let value = "value".to_string();
        cache.set(key.clone(), value.clone()).await;

        // ! Fail when hwm wasn't updated and ping was not sent
        let (fail_t, fail_r) = oneshot::channel();
        cache.index_get(key.clone(), 1, fail_t).await;
        timeout(Duration::from_millis(1000), fail_r).await.unwrap_err();

        // * success when hwm was updated and ping was sent
        let (t, r) = oneshot::channel();
        cache.index_get(key.clone(), 1, t).await;

        let task = tokio::spawn(r);
        hwm.store(1, std::sync::atomic::Ordering::Relaxed);
        cache.ping().await;

        // THEN
        assert_eq!(task.await.unwrap().unwrap(), QueryIO::BulkString(value.clone().into()));
    }
}
