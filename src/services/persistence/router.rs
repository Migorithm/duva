use std::hash::Hasher;

use crate::services::query_manager::{query::Args, value::Value};

use super::{command::PersistCommand, ttl_handlers::set::TtlSetter, CacheDb};
use anyhow::Result;
#[derive(Clone)]
pub struct PersistenceRouter(Vec<tokio::sync::mpsc::Sender<PersistCommand>>);

impl PersistenceRouter {
    pub(crate) async fn route_get(&self, args: &Args) -> Result<Value> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.select_shard(&args)?
            .send(PersistCommand::Get(args.clone(), tx))
            .await?;

        Ok(rx.await?)
    }

    pub(crate) async fn route_set(&self, args: &Args, ttl_sender: TtlSetter) -> Result<()> {
        self.select_shard(&args)?
            .send(PersistCommand::Set(args.clone(), ttl_sender.clone()))
            .await?;
        Ok(())
    }

    fn select_shard(&self, args: &Args) -> Result<&tokio::sync::mpsc::Sender<PersistCommand>> {
        let shard_key = self.take_shard_key_from_args(&args)?;
        Ok(&self[shard_key as usize])
    }

    fn take_shard_key_from_args(&self, args: &Args) -> Result<usize> {
        let key = args.first()?;

        match key {
            Value::BulkString(key) => Ok(self.take_shard_key_from_str(&key)),
            _ => Err(anyhow::anyhow!("Expected key to be a bulk string")),
        }
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
            PersistCommand::Set(args, sender) => {
                // Maybe you have to pass sender?

                let _ = db.handle_set(&args, sender).await;
            }
            PersistCommand::Get(args, sender) => {
                db.handle_get(&args, sender);
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

impl std::ops::Deref for PersistenceRouter {
    type Target = Vec<tokio::sync::mpsc::Sender<PersistCommand>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for PersistenceRouter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
