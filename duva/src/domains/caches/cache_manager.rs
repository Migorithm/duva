use super::cache_objects::CacheValue;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::caches::command::CacheCommand;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::operation_logs::WriteRequest;
use crate::domains::saves::actor::SaveActor;
use crate::domains::saves::actor::SaveTarget;
use crate::domains::saves::endec::StoredDuration;
use crate::make_smart_pointer;
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesOrdered;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinHandle;
use tracing::debug;

#[derive(Clone, Debug)]
pub(crate) struct CacheManager(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheManager, mpsc::Sender<CacheCommand>);

impl CacheManager {
    pub(crate) async fn route_get(&self, key: impl AsRef<str>) -> Result<Option<CacheValue>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let key_ref = key.as_ref();
        self.0.send(CacheCommand::Get { key: key_ref.into(), callback: tx }).await?;

        Ok(rx.await?)
    }

    pub(crate) async fn route_set(
        &self,
        cache_entry: CacheEntry,
        current_idx: u64,
    ) -> Result<String> {
        let value = cache_entry.value().to_string();
        self.0.send(CacheCommand::Set { cache_entry }).await?;
        Ok(format!("s:{}|idx:{}", value, current_idx))
    }

    pub(crate) async fn route_save(
        &self,
        save_target: SaveTarget,
        repl_id: ReplicationId,
        current_offset: u64,
    ) -> Result<JoinHandle<Result<SaveActor>>> {
        let (outbox, inbox) = tokio::sync::mpsc::channel(100);
        let save_actor = SaveActor::new(save_target, repl_id, current_offset).await?;

        let cache_handler = self.0.clone();
        tokio::spawn(async move {
            let _ = cache_handler.send(CacheCommand::Save { outbox }).await;
        });

        //* defaults to BGSAVE but optionally waitable
        Ok(tokio::spawn(save_actor.run(inbox)))
    }

    pub(crate) async fn apply_log(&self, msg: WriteRequest, log_index: u64) -> Result<()> {
        match msg {
            | WriteRequest::Set { key, value, expires_at } => {
                let mut cache_entry = CacheEntry::new(key, value);
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
                self.route_bulk_set(entries).await?;
            },
        };

        // * This is to wake up the cache actors to process the pending read requests
        self.pings().await;

        Ok(())
    }
    async fn pings(&self) {
        let _ = self.0.send(CacheCommand::Ping).await;
    }

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Vec<String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.0.send(CacheCommand::Keys { pattern, callback: tx }).await;
        // TODO handle unwrap
        rx.await.unwrap()
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

    async fn send_selectively<T>(
        &self,
        keys: Vec<String>,
        func: fn(String, Sender<T>) -> CacheCommand,
    ) -> Vec<Result<T, RecvError>> {
        FuturesUnordered::from_iter(keys.into_iter().map(|key| {
            let (tx, rx) = tokio::sync::oneshot::channel();
            async move {
                let _ = self.0.send(func(key, tx)).await;
                rx.await
            }
        }))
        .collect::<Vec<_>>()
        .await
    }

    pub(crate) async fn route_index_get(
        &self,
        key: String,
        index: u64,
    ) -> Result<Option<CacheValue>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0.send(CacheCommand::IndexGet { key, read_idx: index, callback: tx }).await?;

        Ok(rx.await?)
    }

    pub(crate) async fn drop_cache(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.0.send(CacheCommand::Drop { callback: tx }).await;
        let _ = rx.await;
    }

    pub(crate) async fn route_ttl(&self, key: String) -> Result<String> {
        let Ok(Some(CacheValue { expiry: Some(exp), .. })) = self.route_get(key).await else {
            return Ok("-1".to_string());
        };

        let now = Utc::now();
        let ttl_in_sec = exp.signed_duration_since(now).num_seconds();
        let ttl = if ttl_in_sec < 0 { "-1".to_string() } else { ttl_in_sec.to_string() };
        Ok(ttl)
    }

    pub(crate) async fn route_append(&self, key: String, value: String) -> Result<usize> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0.send(CacheCommand::Append { key, value, callback: tx }).await?;
        rx.await?
    }

    pub(crate) async fn route_numeric_delta(
        &self,
        key: String,
        arg: i64,
        current_idx: u64,
    ) -> Result<String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0.send(CacheCommand::NumericDetla { key, delta: arg, callback: tx }).await?;
        let current = rx.await?;
        Ok(format!("s:{}|idx:{}", current?, current_idx))
    }

    async fn route_bulk_set(&self, entries: Vec<CacheEntry>) -> Result<()> {
        let closure = |entry| -> CacheCommand { CacheCommand::Set { cache_entry: entry } };
        FuturesOrdered::from_iter(entries.into_iter().map(|entry| async move {
            let _ = self.0.send(closure(entry)).await;
        }))
        .collect::<Vec<_>>()
        .await;
        Ok(())
    }

    pub(crate) async fn route_mset(&self, cache_entries: Vec<CacheEntry>) {
        join_all(cache_entries.into_iter().map(|entry| async move {
            let _ = self.0.send(CacheCommand::Set { cache_entry: entry }).await;
        }))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::caches::actor::CacheActor;
    use crate::domains::caches::cache_objects::{CacheEntry, CacheValue};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    #[tokio::test]
    async fn test_route_bulk_set() {
        // GIVEN: A CacheManager with cache actors
        let hwm = Arc::new(AtomicU64::new(0));
        let cache_manager = CacheActor::run(hwm);

        // Create many entries
        let entries: Vec<CacheEntry> = (0..50)
            .map(|i| CacheEntry::new(format!("key_{}", i), format!("value_{}", i)))
            .collect();

        // WHEN: We call route_bulk_set
        let result = cache_manager.route_bulk_set(entries).await;

        // THEN: The operation should succeed
        assert!(result.is_ok());

        // AND: All entries should be retrievable
        for i in 0..50 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);

            let retrieved_value = cache_manager.route_get(&key).await.unwrap();
            assert_eq!(retrieved_value, Some(CacheValue::new(expected_value)));
        }
    }

    #[tokio::test]
    async fn test_route_bulk_set_with_expiry() {
        // GIVEN: A CacheManager with cache actors
        let hwm = Arc::new(AtomicU64::new(0));
        let cache_manager = CacheActor::run(hwm);

        // Create entries with expiry times
        let future_time = Utc::now() + chrono::Duration::seconds(10);
        let entries = vec![
            CacheEntry::new("expire_key1", "expire_value1").with_expiry(future_time),
            CacheEntry::new("expire_key2", "expire_value2").with_expiry(future_time),
        ];

        // WHEN: We call route_bulk_set
        let result = cache_manager.route_bulk_set(entries).await;

        // THEN: The operation should succeed
        assert!(result.is_ok());

        // AND: Entries should be retrievable with their expiry times
        let value1 = cache_manager.route_get("expire_key1").await.unwrap();
        assert_eq!(value1.as_ref().unwrap().value, "expire_value1");
        assert!(value1.as_ref().unwrap().expiry.is_some());

        let value2 = cache_manager.route_get("expire_key2").await.unwrap();
        assert_eq!(value2.as_ref().unwrap().value, "expire_value2");
        assert!(value2.as_ref().unwrap().expiry.is_some());
    }
}
