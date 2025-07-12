use super::cache_objects::CacheValue;
use crate::domains::caches::actor::CacheActor;
use crate::domains::caches::actor::CacheCommandSender;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::caches::cache_objects::value::WRONG_TYPE_ERR_MSG;
use crate::domains::caches::command::CacheCommand;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::operation_logs::WriteRequest;
use crate::domains::saves::actor::SaveActor;
use crate::domains::saves::actor::SaveTarget;
use crate::domains::saves::endec::StoredDuration;
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::{hash::Hasher, iter::Zip};
use tokio::sync::oneshot::Sender;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinHandle;
use tracing::debug;

type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
type OneShotReceiverJoinHandle<T> =
    tokio::task::JoinHandle<std::result::Result<T, tokio::sync::oneshot::error::RecvError>>;

#[derive(Clone, Debug)]
pub(crate) struct CacheManager {
    pub(crate) inboxes: Vec<CacheCommandSender>,
}

impl CacheManager {
    pub(crate) fn run_cache_actors(hwm: Arc<AtomicU64>) -> CacheManager {
        const NUM_OF_PERSISTENCE: usize = 10;
        CacheManager {
            inboxes: (0..NUM_OF_PERSISTENCE)
                .map(|_| CacheActor::run(hwm.clone()))
                .collect::<Vec<_>>(),
        }
    }

    pub(crate) async fn route_get(&self, key: impl AsRef<str>) -> Result<CacheValue> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let key_ref = key.as_ref();
        self.select_shard(key_ref)
            .send(CacheCommand::Get { key: key_ref.into(), callback: tx })
            .await?;
        let res = rx.await?;
        if !res.is_string() && !res.is_null() {
            return Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG));
        }
        Ok(res)
    }

    pub(crate) async fn route_set(
        &self,
        cache_entry: CacheEntry,
        current_idx: u64,
    ) -> Result<String> {
        let value = cache_entry.as_str()?;
        self.select_shard(cache_entry.key()).send(CacheCommand::Set { cache_entry }).await?;
        Ok(IndexedValueCodec::encode(value, current_idx))
    }

    pub(crate) async fn route_mset(&self, cache_entries: Vec<CacheEntry>) {
        join_all(cache_entries.into_iter().map(|entry| async move {
            let _ =
                self.select_shard(entry.key()).send(CacheCommand::Set { cache_entry: entry }).await;
        }))
        .await;
    }

    pub(crate) async fn route_lpush(
        &self,
        key: String,
        value: Vec<String>,
        current_idx: u64,
    ) -> Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::LPush { key, values: value, callback: tx.into() })
            .await?;
        let current_len = rx.await??;
        Ok(IndexedValueCodec::encode(current_len, current_idx))
    }

    pub(crate) async fn route_lpop(&self, key: String, count: usize) -> Result<Vec<String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::LPop { key, count, callback: tx.into() })
            .await?;

        let pop_values = rx.await?;
        Ok(pop_values)
    }

    pub(crate) async fn route_rpush(
        &self,
        key: String,
        value: Vec<String>,
        unwrap: u64,
    ) -> Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::RPush { key, values: value, callback: tx.into() })
            .await?;
        let current_len = rx.await??;
        Ok(IndexedValueCodec::encode(current_len, unwrap))
    }

    pub(crate) async fn route_rpop(&self, key: String, count: usize) -> Result<Vec<String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::RPop { key, count, callback: tx.into() })
            .await?;

        let pop_values = rx.await?;
        Ok(pop_values)
    }

    pub(crate) async fn route_save(
        &self,
        save_target: SaveTarget,
        repl_id: ReplicationId,
        current_offset: u64,
    ) -> Result<JoinHandle<Result<SaveActor>>> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let save_actor =
            SaveActor::new(save_target, self.inboxes.len(), repl_id, current_offset).await?;

        // get all the handlers to cache actors
        for cache_handler in self.inboxes.iter().map(Clone::clone) {
            let outbox = outbox.clone();
            tokio::spawn(async move {
                let _ = cache_handler.send(CacheCommand::Save { outbox }).await;
            });
        }

        //* defaults to BGSAVE but optionally waitable
        Ok(tokio::spawn(save_actor.run(inbox)))
    }

    pub(crate) async fn apply_log(&self, msg: WriteRequest, log_index: u64) -> Result<()> {
        match msg {
            | WriteRequest::Set { key, value, expires_at } => {
                let mut cache_entry = CacheEntry::new(key, value.as_str());
                if let Some(expires_at) = expires_at {
                    cache_entry = cache_entry
                        .with_expiry(StoredDuration::Milliseconds(expires_at).to_datetime());
                }
                self.route_set(cache_entry, log_index).await?;
            },
            | WriteRequest::Delete { keys } => {
                self.route_delete(keys).await?;
            },
            | WriteRequest::Append { key, value } => {
                self.route_append(key, value).await?;
            },
            | WriteRequest::Decr { key, delta } => {
                self.route_numeric_delta(key, -delta, log_index).await?;
            },
            | WriteRequest::Incr { key, delta } => {
                self.route_numeric_delta(key, delta, log_index).await?;
            },
            | WriteRequest::MSet { entries } => {
                self.route_mset(entries).await;
            },
            | WriteRequest::LPush { key, value } => {
                self.route_lpush(key, value, log_index).await?;
            },
            | WriteRequest::LPop { key, count } => {
                self.route_lpop(key, count).await?;
            },
            | WriteRequest::RPush { key, value } => {
                self.route_rpush(key, value, log_index).await?;
            },
        };

        // * This is to wake up the cache actors to process the pending read requests
        self.pings().await;

        Ok(())
    }
    async fn pings(&self) {
        join_all(self.inboxes.iter().map(|shard| shard.send(CacheCommand::Ping))).await;
    }

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Vec<String> {
        let (senders, receivers) = self.oneshot_channels();

        // send keys to shards
        self.chain(senders).for_each(|(shard, sender)| {
            tokio::spawn(Self::send_keys_to_shard(shard.clone(), pattern.clone(), sender));
        });
        let mut res = Vec::new();
        for v in receivers {
            if let Ok(Ok(keys)) = v.await {
                res.extend(keys)
            }
        }
        res
    }
    pub(crate) async fn apply_snapshot(self, key_values: Vec<CacheEntry>) -> Result<()> {
        // * Here, no need to think about index as it is to update state and no return is required
        join_all(
            key_values
                .into_iter()
                .filter(|kvc| kvc.is_valid(&Utc::now()))
                .map(|kvs| self.route_set(kvs, 0)),
        )
        .await;

        let keys = self.route_keys(None).await;
        debug!("Full Sync Keys: {:?}", keys);

        Ok(())
    }

    // Send recv handler firstly to the background and return senders and join handlers for receivers
    fn oneshot_channels<T: Send + Sync + 'static>(
        &self,
    ) -> (Vec<OneShotSender<T>>, Vec<OneShotReceiverJoinHandle<T>>) {
        (0..self.inboxes.len())
            .map(|_| {
                let (sender, recv) = tokio::sync::oneshot::channel();
                let recv_handler = tokio::spawn(recv);
                (sender, recv_handler)
            })
            .unzip()
    }

    fn chain<T>(
        &self,
        senders: Vec<Sender<T>>,
    ) -> Zip<std::slice::Iter<'_, CacheCommandSender>, std::vec::IntoIter<Sender<T>>> {
        self.inboxes.iter().zip(senders)
    }

    // stateless function to send keys
    async fn send_keys_to_shard(
        shard: CacheCommandSender,
        pattern: Option<String>,
        tx: OneShotSender<Vec<String>>,
    ) -> Result<()> {
        Ok(shard.send(CacheCommand::Keys { pattern: pattern.clone(), callback: tx }).await?)
    }

    pub(crate) async fn route_delete(&self, keys: Vec<String>) -> Result<u64> {
        let closure = |key, callback| -> CacheCommand { CacheCommand::Delete { key, callback } };
        // Create futures for all delete operations at once
        let results = self.send_selectively(keys, closure).await;

        let deleted = results.into_iter().filter_map(|r| r.ok().filter(|&success| success)).count();
        Ok(deleted as u64)
    }
    pub(crate) async fn route_exists(&self, keys: Vec<String>) -> Result<u64> {
        let closure = |key, callback| -> CacheCommand { CacheCommand::Exists { key, callback } };
        // Create futures for all delete operations at once
        let results = self.send_selectively(keys, closure).await;

        let found = results.into_iter().filter_map(|r| r.ok().filter(|&success| success)).count();
        Ok(found as u64)
    }

    pub(crate) fn select_shard(&self, key: &str) -> &CacheCommandSender {
        let shard_key = self.take_shard_key_from_str(key);
        &self.inboxes[shard_key]
    }

    async fn send_selectively<T>(
        &self,
        keys: Vec<String>,
        func: fn(String, Sender<T>) -> CacheCommand,
    ) -> Vec<Result<T, RecvError>> {
        FuturesUnordered::from_iter(keys.into_iter().map(|key| {
            let (tx, rx) = tokio::sync::oneshot::channel();
            async move {
                let _ = self.select_shard(&key).send(func(key, tx)).await;
                rx.await
            }
        }))
        .collect::<Vec<_>>()
        .await
    }

    fn take_shard_key_from_str(&self, s: &str) -> usize {
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&s, &mut hasher);
        hasher.finish() as usize % self.inboxes.len()
    }

    pub(crate) async fn route_index_get(&self, key: String, index: u64) -> Result<CacheValue> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::IndexGet { key, read_idx: index, callback: tx })
            .await?;

        Ok(rx.await?)
    }
    pub(crate) async fn route_mget(&self, keys: Vec<String>) -> Vec<Option<CacheEntry>> {
        let futures = keys.into_iter().map(|key| {
            let shard = self.select_shard(&key).clone();
            tokio::spawn(async move {
                let (tx, rx) = tokio::sync::oneshot::channel();
                shard.send(CacheCommand::Get { key: key.clone(), callback: tx }).await.ok()?;
                let value = rx.await.ok()?;
                Some(CacheEntry::new_with_cache_value(key, value))
            })
        });

        join_all(futures).await.into_iter().filter_map(Result::ok).collect()
    }

    pub(crate) async fn drop_cache(&self) {
        let (txs, rxs) = self.oneshot_channels();
        join_all(
            self.chain(txs)
                .map(|(shard, sender)| shard.send(CacheCommand::Drop { callback: sender })),
        )
        .await;

        join_all(rxs.into_iter()).await;
    }

    pub(crate) async fn route_ttl(&self, key: String) -> Result<String> {
        let Ok(CacheValue { expiry: Some(exp), .. }) = self.route_get(key).await else {
            return Ok("-1".to_string());
        };

        let now = Utc::now();
        let ttl_in_sec = exp.signed_duration_since(now).num_seconds();
        let ttl = if ttl_in_sec < 0 { "-1".to_string() } else { ttl_in_sec.to_string() };
        Ok(ttl)
    }

    pub(crate) async fn route_append(&self, key: String, value: String) -> Result<usize> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(key.as_str())
            .send(CacheCommand::Append { key, value, callback: tx })
            .await?;
        rx.await?
    }

    pub(crate) async fn route_numeric_delta(
        &self,
        key: String,
        arg: i64,
        current_idx: u64,
    ) -> Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(key.as_str())
            .send(CacheCommand::NumericDetla { key, delta: arg, callback: tx })
            .await?;
        let current = rx.await?;
        Ok(IndexedValueCodec::encode(current?, current_idx))
    }

    pub(crate) async fn route_llen(&self, key: String) -> Result<usize> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key).send(CacheCommand::LLen { key, callback: tx.into() }).await?;
        rx.await?
    }

    pub(crate) async fn route_lrange(
        &self,
        key: String,
        start: isize,
        end: isize,
    ) -> Result<Vec<String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::LRange { key, start, end, callback: tx.into() })
            .await?;
        rx.await?
    }

    pub(crate) async fn route_ltrim(&self, key: String, start: isize, end: isize) -> Result<()> {
        todo!()
    }
}

pub struct IndexedValueCodec;
impl IndexedValueCodec {
    pub fn decode_value(s: std::borrow::Cow<'_, str>) -> Option<i64> {
        s.split('|').next().and_then(|s| s.rsplit(':').next()).and_then(|id| id.parse::<i64>().ok())
    }

    pub fn decode_index(s: std::borrow::Cow<'_, str>) -> Option<u64> {
        s.rsplit('|')
            .next()
            .and_then(|s| s.rsplit(':').next())
            .and_then(|id| id.parse::<u64>().ok())
    }

    pub fn encode<T>(value: T, idx: u64) -> String
    where
        T: Display,
    {
        format!("s:{value}|idx:{idx}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::caches::cache_objects::{CacheEntry, CacheValue};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    #[tokio::test]
    async fn test_route_bulk_set_distribution_across_shards() {
        // GIVEN: A CacheManager with cache actors
        let hwm = Arc::new(AtomicU64::new(0));
        let cache_manager = CacheManager::run_cache_actors(hwm);

        // Create many entries that should be distributed across different shards
        let entries: Vec<CacheEntry> = (0..50)
            .map(|i| CacheEntry::new(format!("key_{i}"), format!("value_{i}").as_str()))
            .collect();

        // WHEN: We call route_bulk_set
        cache_manager.route_mset(entries).await;

        // THEN
        // AND: All entries should be retrievable
        for i in 0..50 {
            let key = format!("key_{i}");
            let expected_value = format!("value_{i}");

            let retrieved_value = cache_manager.route_get(&key).await.unwrap();
            assert_eq!(retrieved_value, CacheValue::new(expected_value.as_str()));
        }
    }

    #[tokio::test]
    async fn test_route_bulk_set_with_expiry() {
        // GIVEN: A CacheManager with cache actors
        let hwm = Arc::new(AtomicU64::new(0));
        let cache_manager = CacheManager::run_cache_actors(hwm);

        // Create entries with expiry times
        let future_time = Utc::now() + chrono::Duration::seconds(10);
        let entries = vec![
            CacheEntry::new("expire_key1", "expire_value1").with_expiry(future_time),
            CacheEntry::new("expire_key2", "expire_value2").with_expiry(future_time),
        ];

        // WHEN: We call route_bulk_set
        cache_manager.route_mset(entries).await;

        // THEN: The operation should succeed

        // AND: Entries should be retrievable with their expiry times
        let value1 = cache_manager.route_get("expire_key1").await.unwrap();
        assert_eq!(value1.value, "expire_value1");
        assert!(value1.expiry.is_some());

        let value2 = cache_manager.route_get("expire_key2").await.unwrap();
        assert_eq!(value2.value, "expire_value2");
        assert!(value2.expiry.is_some());
    }
}
