use super::{command::PersistCommand, ttl_handlers::set::TtlSetter, CacheDb};
use crate::{make_smart_pointer, services::value::Value};
use anyhow::Result;
use std::hash::Hasher;

type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
type OneShotReceiverJoinHandle<T> =
    tokio::task::JoinHandle<std::result::Result<T, tokio::sync::oneshot::error::RecvError>>;

#[derive(Clone)]
pub struct CacheDbMessageRouter(Vec<tokio::sync::mpsc::Sender<PersistCommand>>);

impl CacheDbMessageRouter {
    pub(crate) async fn route_get(&self, key: String) -> Result<Value> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&key)?
            .send(PersistCommand::Get { key, sender: tx })
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
            .send(PersistCommand::Set {
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
        shard: tokio::sync::mpsc::Sender<PersistCommand>,
        pattern: Option<String>,
        tx: OneShotSender<Value>,
    ) -> Result<()> {
        Ok(shard
            .send(PersistCommand::Keys {
                pattern: pattern.clone(),
                sender: tx,
            })
            .await?)
    }

    fn select_shard(&self, key: &str) -> Result<&tokio::sync::mpsc::Sender<PersistCommand>> {
        let shard_key = self.take_shard_key_from_str(&key);
        Ok(&self[shard_key as usize])
    }

    pub(crate) fn take_shard_key_from_str(&self, s: &str) -> usize {
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&s, &mut hasher);
        hasher.finish() as usize % self.len()
    }
}

async fn persist_actor(mut recv: tokio::sync::mpsc::Receiver<PersistCommand>) -> Result<()> {
    // inner state
    let mut db = CacheDb::default();

    while let Some(command) = recv.recv().await {
        match command {
            PersistCommand::StartUp(cache_db) => db = cache_db,
            PersistCommand::StopSentinel => break,
            PersistCommand::Set {
                key,
                value,
                expiry,
                ttl_sender,
            } => {
                // Maybe you have to pass sender?
                let _ = db.handle_set(key, value, expiry, ttl_sender).await;
            }
            PersistCommand::Get { key, sender } => {
                db.handle_get(key, sender);
            }
            PersistCommand::Keys { pattern, sender } => {
                db.handle_keys(pattern, sender);
            }
            PersistCommand::Delete(key) => db.handle_delete(&key),
        }
    }
    Ok(())
}
pub fn run_persistent_actors(num_of_actors: usize) -> CacheDbMessageRouter {
    let mut persistence_senders = CacheDbMessageRouter(Vec::with_capacity(num_of_actors));

    (0..num_of_actors).for_each(|_| {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(persist_actor(rx));
        persistence_senders.push(tx);
    });

    persistence_senders
}

make_smart_pointer!(
    CacheDbMessageRouter,
    Vec<tokio::sync::mpsc::Sender<PersistCommand>>
);
