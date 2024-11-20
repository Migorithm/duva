use crate::{
    adapters::persistence::{bytes_handler::BytesEndec, Init},
    config::Config,
    services::{
        statefuls::{
            command::CacheCommand,
            ttl_handlers::{
                delete_actor::TtlDeleteActor,
                set::{TtlInbox, TtlSetActor},
            },
        },
        value::Value,
    },
};
use anyhow::Result;
use std::{hash::Hasher, iter::Zip, sync::Arc};
use tokio::sync::oneshot::Sender;

use super::cache_actor::{CacheActor, CacheMessageInbox};
type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
type OneShotReceiverJoinHandle<T> =
    tokio::task::JoinHandle<std::result::Result<T, tokio::sync::oneshot::error::RecvError>>;

#[derive(Clone)]
pub(crate) struct CacheDispatcher {
    pub(crate) inboxes: Vec<CacheMessageInbox>,
    pub(crate) config: Arc<Config>,
}

impl CacheDispatcher {
    pub(crate) async fn load_data(&self, ttl_inbox: TtlInbox) -> Result<()> {
        let Ok(filepath) = self.config.parse_filepath().await else {
            return Ok(());
        };

        let data = tokio::fs::read(filepath).await?;
        let data: BytesEndec<Init> = data.as_slice().into();
        let database = data.load_header()?.load_metadata()?.load_database()?;

        for kvs in database.key_values() {
            self.route_set(kvs.key, kvs.value, kvs.expiry, ttl_inbox.clone())
                .await?;
        }

        Ok(())
    }

    pub(crate) async fn route_get(&self, key: String) -> Result<Value> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)?
            .send(CacheCommand::Get { key, sender: tx })
            .await?;

        Ok(rx.await?)
    }

    pub(crate) async fn route_set(
        &self,
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlInbox,
    ) -> Result<Value> {
        self.select_shard(&key)?
            .send(CacheCommand::Set {
                key,
                value,
                expiry,
                ttl_sender,
            })
            .await?;
        Ok(Value::SimpleString("OK".to_string()))
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

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Result<Value> {
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
                Ok(Ok(Value::Array(v))) => keys.extend(v),
                _ => continue,
            }
        }

        Ok(Value::Array(keys))
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
        tx: OneShotSender<Value>,
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
                .map(|actor_id| CacheActor::run(actor_id))
                .collect::<Vec<_>>()
                .into(),
            config,
        };

        let ttl_inbox = cache_dispatcher.run_ttl_actors();
        (cache_dispatcher, ttl_inbox)
    }

    fn run_ttl_actors(&self) -> TtlInbox {
        TtlDeleteActor::run(self);
        let ttl_setter = TtlSetActor::run();
        ttl_setter
    }
}
