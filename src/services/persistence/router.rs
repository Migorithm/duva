use crate::services::query_manager::{query::Args, value::Value};

use super::{hasher::get_hash, CacheDb, PersistEnum};
use anyhow::Result;
#[derive(Clone)]
pub struct PersistenceRouter(Vec<tokio::sync::mpsc::Sender<PersistEnum>>);

impl PersistenceRouter {
    pub(crate) fn take_shard_key(&self, args: &Args) -> Result<usize> {
        let key = args.first()?;

        match key {
            Value::BulkString(key) => Ok(get_hash(&key).shard_key(self.len())),
            _ => Err(anyhow::anyhow!("Expected key to be a bulk string")),
        }
    }
}

async fn persist_actor(mut recv: tokio::sync::mpsc::Receiver<PersistEnum>) -> Result<()> {
    // inner state
    let mut db = CacheDb::default();

    while let Some(command) = recv.recv().await {
        match command {
            PersistEnum::StopSentinel => break,
            PersistEnum::Set(args, sender) => {
                // Maybe you have to pass sender?

                let _ = db.handle_set(&args, sender).await;
            }
            PersistEnum::Get(args, sender) => {
                db.handle_get(&args, sender);
            }
            PersistEnum::Delete(key) => db.handle_delete(&key),
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
    type Target = Vec<tokio::sync::mpsc::Sender<PersistEnum>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for PersistenceRouter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
