use crate::services::statefuls::{
    command::{AOFCommand, CacheCommand},
    CacheDb,
};
use anyhow::Result;

use super::{aof_actor::run_aof_actor, inmemory_router::CacheDbMessageRouter};

struct CacheActor {
    db: CacheDb,
    actor_id: usize,
    inbox: tokio::sync::mpsc::Receiver<CacheCommand>,
    outbox: tokio::sync::mpsc::Sender<AOFCommand>,
}
impl CacheActor {
    // Create a new CacheActor with inner state
    fn run(actor_id: usize) -> tokio::sync::mpsc::Sender<CacheCommand> {
        let (outbox, aof_actor_inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(run_aof_actor(aof_actor_inbox, actor_id));

        let (tx, cache_actor_inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(
            Self {
                db: Default::default(),
                inbox: cache_actor_inbox,
                actor_id,
                outbox,
            }
            .start(),
        );

        tx
    }

    async fn start(mut self) -> Result<()> {
        while let Some(command) = self.inbox.recv().await {
            match command {
                CacheCommand::StartUp(cache_db) => self.db = cache_db,
                CacheCommand::StopSentinel => break,
                CacheCommand::Set {
                    key,
                    value,
                    expiry,
                    ttl_sender,
                } => {
                    // Maybe you have to pass sender?

                    let _ = tokio::join!(
                        self.db
                            .handle_set(key.clone(), value.clone(), expiry, ttl_sender.clone()),
                        self.outbox.send(AOFCommand::Set {
                            key: key.clone(),
                            value: value.clone(),
                            expiry,
                        })
                    );
                }
                CacheCommand::Get { key, sender } => {
                    self.db.handle_get(key, sender);
                }
                CacheCommand::Keys { pattern, sender } => {
                    self.db.handle_keys(pattern, sender);
                }
                CacheCommand::Delete(key) => self.db.handle_delete(&key),
            }
        }
        Ok(())
    }
}

pub fn run_cache_actors(num_of_actors: usize) -> CacheDbMessageRouter {
    let mut cache_senders = CacheDbMessageRouter::new(num_of_actors);

    (0..num_of_actors).for_each(|actor_id| {
        let sender = CacheActor::run(actor_id);
        cache_senders.push(sender);
    });

    cache_senders
}
