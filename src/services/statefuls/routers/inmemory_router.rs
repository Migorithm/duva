use crate::{
    make_smart_pointer,
    services::{
        statefuls::{command::CacheCommand, ttl_handlers::set::TtlSetter},
        value::Value,
    },
};
use anyhow::Result;
use std::hash::Hasher;
type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
type OneShotReceiverJoinHandle<T> =
    tokio::task::JoinHandle<std::result::Result<T, tokio::sync::oneshot::error::RecvError>>;

#[derive(Clone)]
pub struct CacheDispatcher(Vec<tokio::sync::mpsc::Sender<CacheCommand>>);

impl CacheDispatcher {
    pub(crate) fn new(num_of_actors: usize) -> Self {
        CacheDispatcher(Vec::with_capacity(num_of_actors))
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
        ttl_sender: TtlSetter,
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

    // stateless function to create oneshot channels that maps to the number of shards
    fn ontshot_channels<T: Send + Sync + 'static>(
        &self,
    ) -> (Vec<OneShotSender<T>>, Vec<OneShotReceiverJoinHandle<T>>) {
        let mut senders = Vec::with_capacity(self.len());
        let mut receivers = Vec::with_capacity(self.len());
        (0..self.len()).for_each(|_| {
            let (tx, rx) = tokio::sync::oneshot::channel();
            senders.push(tx);
            receivers.push(rx);
        });

        // Fire listeners first before sending
        let mut receiver_handles = Vec::new();
        for recv in receivers {
            receiver_handles.push(tokio::spawn(recv))
        }
        (senders, receiver_handles)
    }

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Result<Value> {
        let (senders, receiver_handles) = self.ontshot_channels();

        // send keys to shards
        for (shard, tx) in self.iter().zip(senders.into_iter()) {
            tokio::spawn(Self::send_keys_to_shard(shard.clone(), pattern.clone(), tx));
        }

        let mut keys = Vec::new();
        for v in receiver_handles {
            match v.await {
                Ok(Ok(Value::Array(v))) => keys.extend(v),
                _ => continue,
            }
        }

        Ok(Value::Array(keys))
    }

    // stateless function to send keys
    async fn send_keys_to_shard(
        shard: tokio::sync::mpsc::Sender<CacheCommand>,
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

    fn select_shard(&self, key: &str) -> Result<&tokio::sync::mpsc::Sender<CacheCommand>> {
        let shard_key = self.take_shard_key_from_str(&key);
        Ok(&self[shard_key as usize])
    }

    pub(crate) fn take_shard_key_from_str(&self, s: &str) -> usize {
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&s, &mut hasher);
        hasher.finish() as usize % self.len()
    }
}

make_smart_pointer!(
    CacheDispatcher,
    Vec<tokio::sync::mpsc::Sender<CacheCommand>>
);
