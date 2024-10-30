use super::{command::PersistCommand, ttl_handlers::set::TtlSetter, CacheDb};
use crate::{make_smart_pointer, services::value::Value};
use anyhow::Result;
use std::hash::Hasher;

#[derive(Clone)]
pub struct PersistenceRouter(Vec<tokio::sync::mpsc::Sender<PersistCommand>>);

impl PersistenceRouter {
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

    pub(crate) async fn route_keys(&self, pattern: Option<String>) -> Result<Value> {
        let listeners = self.broadcast_keys(pattern).await?;
        let mut keys = Vec::new();
        //TODO join all
        for l in listeners {
            match l.await? {
                Value::Array(v) => keys.extend(v),
                _ => {}
            }
        }
        Ok(Value::Array(keys))
    }

    async fn broadcast_keys(
        &self,
        pattern: Option<String>,
    ) -> Result<Vec<tokio::sync::oneshot::Receiver<Value>>> {
        let mut listeners = Vec::with_capacity(self.len());

        for shard in self.iter() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            shard
                .send(PersistCommand::Keys {
                    pattern: pattern.clone(),
                    sender: tx,
                })
                .await?;
            listeners.push(rx);
        }
        Ok(listeners)
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
pub fn run_persistent_actors(num_of_actors: usize) -> PersistenceRouter {
    let mut persistence_senders = PersistenceRouter(Vec::with_capacity(num_of_actors));

    (0..num_of_actors).for_each(|_| {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(persist_actor(rx));
        persistence_senders.push(tx);
    });
    persistence_senders
}

make_smart_pointer!(
    PersistenceRouter,
    Vec<tokio::sync::mpsc::Sender<PersistCommand>>
);
