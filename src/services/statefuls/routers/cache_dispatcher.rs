use super::cache_actor::{CacheActor, CacheCommand, CacheMessageInbox};
use super::save_actor::SaveActor;
use super::ttl_actor::{TtlActor, TtlInbox};
use crate::adapters::persistence::{byte_decoder::BytesDecoder, Init};
use crate::config::Config;

use crate::services::query_io::QueryIO;
use crate::services::CacheEntry;
use anyhow::Result;
use std::{hash::Hasher, iter::Zip, sync::Arc, time::SystemTime};
use tokio::sync::oneshot::Sender;

type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
type OneShotReceiverJoinHandle<T> =
    tokio::task::JoinHandle<std::result::Result<T, tokio::sync::oneshot::error::RecvError>>;

#[derive(Clone)]
pub(crate) struct CacheDispatcher {
    pub(crate) inboxes: Vec<CacheMessageInbox>,
    pub(crate) config: Arc<Config>,
}

impl CacheDispatcher {
    pub(crate) async fn load_data(
        &self,
        ttl_inbox: TtlInbox,
        current_system_time: SystemTime,
    ) -> Result<()> {
        let Ok(Some(filepath)) = self.config.try_filepath().await else {
            return Ok(());
        };

        let data = tokio::fs::read(filepath).await?;
        let decoder: BytesDecoder<Init> = data.as_slice().into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;

        for kvs in database
            .key_values()
            .into_iter()
            .filter(|kvs| kvs.is_valid(&current_system_time))
        {
            self.route_set(kvs, ttl_inbox.clone()).await?;
        }

        Ok(())
    }

    pub(crate) async fn route_get(&self, key: String) -> Result<QueryIO> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)?
            .send(CacheCommand::Get { key, sender: tx })
            .await?;

        Ok(rx.await?)
    }

    pub(crate) async fn route_set(&self, kvs: CacheEntry, ttl_sender: TtlInbox) -> Result<QueryIO> {
        self.select_shard(kvs.key())?
            .send(CacheCommand::Set {
                cache_entry: kvs,
                ttl_sender,
            })
            .await?;
        Ok(QueryIO::SimpleString("OK".to_string()))
    }

    // Send recv handler firstly to the background and return senders and join handlers for receivers
    fn ontshot_channels<T: Send + Sync + 'static>(
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

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Result<QueryIO> {
        let (senders, receivers) = self.ontshot_channels();

        // send keys to shards
        self.chain(senders).for_each(|(shard, sender)| {
            tokio::spawn(Self::send_keys_to_shard(
                shard.clone(),
                pattern.clone(),
                sender,
            ));
        });

        let mut keys = Vec::new();
        for v in receivers {
            match v.await {
                Ok(Ok(QueryIO::Array(v))) => keys.extend(v),
                _ => continue,
            }
        }

        Ok(QueryIO::Array(keys))
    }

    fn chain<T>(
        &self,
        senders: Vec<Sender<T>>,
    ) -> Zip<std::slice::Iter<'_, CacheMessageInbox>, std::vec::IntoIter<Sender<T>>> {
        self.inboxes.iter().zip(senders.into_iter())
    }

    // stateless function to send keys
    async fn send_keys_to_shard(
        shard: CacheMessageInbox,
        pattern: Option<String>,
        tx: OneShotSender<QueryIO>,
    ) -> Result<()> {
        Ok(shard
            .send(CacheCommand::Keys {
                pattern: pattern.clone(),
                sender: tx,
            })
            .await?)
    }

    fn select_shard(&self, key: &str) -> Result<&CacheMessageInbox> {
        let shard_key = self.take_shard_key_from_str(&key);
        Ok(&self.inboxes[shard_key as usize])
    }

    pub(crate) fn take_shard_key_from_str(&self, s: &str) -> usize {
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&s, &mut hasher);
        hasher.finish() as usize % self.inboxes.len()
    }

    pub fn run_cache_actors(
        num_of_actors: usize,
        config: Arc<Config>,
    ) -> (CacheDispatcher, TtlInbox) {
        let cache_dispatcher = CacheDispatcher {
            inboxes: (0..num_of_actors)
                .into_iter()
                .map(|_| CacheActor::run())
                .collect::<Vec<_>>()
                .into(),
            config,
        };

        let ttl_inbox = cache_dispatcher.run_ttl_actors();
        (cache_dispatcher, ttl_inbox)
    }

    fn run_ttl_actors(&self) -> TtlInbox {
        let ttl_actor = TtlActor::run(self.clone());
        ttl_actor
    }

    pub fn run_save_actor(&self, db_filepath: Option<String>) {
        let filepath: String = db_filepath.unwrap_or_else(|| "dump.rdb".to_string());
        let outbox = SaveActor::run(filepath, self.inboxes.len());

        // get all the handlers to cache actors
        for inbox in self.inboxes.iter().map(Clone::clone) {
            let outbox = outbox.clone();
            tokio::spawn(async move {
                let _ = inbox.send(CacheCommand::Save { outbox }).await;
            });
        }
    }
}
