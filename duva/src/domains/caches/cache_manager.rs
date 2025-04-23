use crate::domains::append_only_files::WriteRequest;
use crate::domains::caches::actor::CacheActor;
use crate::domains::caches::actor::CacheCommandSender;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::caches::command::CacheCommand;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::query_parsers::QueryIO;
use crate::domains::saves::actor::SaveActor;
use crate::domains::saves::actor::SaveTarget;
use crate::domains::saves::endec::StoredDuration;
use crate::domains::saves::snapshot::Snapshot;
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use tokio::sync::oneshot::error::RecvError;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use std::{hash::Hasher, iter::Zip};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::cache_objects::CacheValue;

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

    pub(crate) async fn route_get(&self, key: impl AsRef<str>) -> Result<Option<CacheValue>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let key_ref = key.as_ref();
        self.select_shard(key_ref)
            .send(CacheCommand::Get { key: key_ref.into(), callback: tx })
            .await?;

        Ok(rx.await?)
    }

    pub(crate) async fn route_set(&self, kvs: CacheEntry) -> Result<()> {
        self.select_shard(kvs.key()).send(CacheCommand::Set { cache_entry: kvs }).await?;
        Ok(())
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

    pub(crate) async fn apply_log(&self, msg: WriteRequest) -> Result<()> {
        match msg {
            WriteRequest::Set { key, value } => {
                self.route_set(CacheEntry::KeyValue { key, value }).await?;
            },
            WriteRequest::SetWithExpiry { key, value, expires_at } => {
                self.route_set(CacheEntry::KeyValueExpiry {
                    key,
                    value,
                    expiry: StoredDuration::Milliseconds(expires_at).to_datetime(),
                })
                .await?;
            },
            WriteRequest::Delete { keys } => {
                self.route_delete(keys).await?;
            },
        };

        self.pings().await;

        Ok(())
    }
    async fn pings(&self) {
        join_all(self.inboxes.iter().map(|shard| shard.send(CacheCommand::Ping))).await;
    }

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Result<QueryIO> {
        let (senders, receivers) = self.oneshot_channels();

        // send keys to shards
        self.chain(senders).for_each(|(shard, sender)| {
            tokio::spawn(Self::send_keys_to_shard(shard.clone(), pattern.clone(), sender));
        });
        let mut keys = Vec::new();
        for v in receivers {
            if let Ok(QueryIO::Array(v)) = v.await? {
                keys.extend(v)
            }
        }
        Ok(QueryIO::Array(keys))
    }
    pub(crate) async fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        join_all(
            snapshot
                .key_values()
                .into_iter()
                .filter(|kvc| kvc.is_valid(&Utc::now()))
                .map(|kvs| self.route_set(kvs)),
        )
        .await;

        // TODO let's find the way to test without adding the following code - echo
        // Only for debugging and test
        if let Ok(QueryIO::Array(data)) = self.route_keys(None).await {
            let mut keys = vec![];
            for key in data {
                let QueryIO::BulkString(key) = key else {
                    continue;
                };
                keys.push(key);
            }
            println!("[INFO] Full Sync Keys: {:?}", keys);
        }
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
        tx: OneShotSender<QueryIO>,
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

    pub(crate) async fn route_index_get(
        &self,
        key: String,
        index: u64,
    ) -> Result<Option<CacheValue>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)
            .send(CacheCommand::IndexGet { key, read_idx: index, callback: tx })
            .await?;

        Ok(rx.await?)
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
        let Ok(Some(CacheValue::ValueWithExpiry { expiry, .. })) = self.route_get(key).await else {
            return Ok("-1".to_string());
        };

        let now = Utc::now();
        let ttl_in_sec = expiry.signed_duration_since(now).num_seconds();
        let ttl = if ttl_in_sec < 0 { "-1".to_string() } else { ttl_in_sec.to_string() };
        Ok(ttl)
    }
}
