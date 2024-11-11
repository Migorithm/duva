use super::{aof_actor::AOFActor, inmemory_router::CacheDbMessageRouter};
use crate::{
    make_smart_pointer,
    services::{
        statefuls::{
            command::{AOFCommand, CacheCommand},
            ttl_handlers::set::TtlSetter,
        },
        value::Value,
    },
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::oneshot;

#[derive(Default)]
pub struct CacheDb(HashMap<String, String>);

impl CacheDb {
    pub async fn handle_set(
        &mut self,
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlSetter,
    ) -> Result<Value> {
        match expiry {
            Some(expiry) => {
                self.insert(key.clone(), value.clone());
                ttl_sender.set_ttl(key.clone(), expiry).await;
            }
            None => {
                self.insert(key.clone(), value.clone());
            }
        }
        Ok(Value::SimpleString("OK".to_string()))
    }

    pub fn handle_get(&self, key: String, sender: oneshot::Sender<Value>) {
        let _ = sender.send(self.get(&key).cloned().into());
    }

    fn handle_delete(&mut self, key: &str) {
        self.remove(key);
    }

    fn handle_keys(&self, pattern: Option<String>, sender: oneshot::Sender<Value>) {
        let ks = self
            .keys()
            .filter(|k| match &pattern {
                // TODO better way to to matching?
                Some(pattern) => k.contains(pattern),
                None => true,
            })
            .map(|k| Value::BulkString(k.clone()))
            .collect::<Vec<_>>();
        sender.send(Value::Array(ks)).unwrap();
    }
}

make_smart_pointer!(CacheDb, HashMap<String, String>);

pub struct CacheActor {
    cache: CacheDb,
    actor_id: usize,
    inbox: tokio::sync::mpsc::Receiver<CacheCommand>,
    outbox: tokio::sync::mpsc::Sender<AOFCommand>,
}
impl CacheActor {
    // Create a new CacheActor with inner state
    fn run(actor_id: usize) -> tokio::sync::mpsc::Sender<CacheCommand> {
        let outbox = AOFActor::run(actor_id);

        let (tx, cache_actor_inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(
            Self {
                cache: Default::default(),
                inbox: cache_actor_inbox,
                actor_id,
                outbox,
            }
            .handle(),
        );
        tx
    }

    async fn handle(mut self) -> Result<()> {
        while let Some(command) = self.inbox.recv().await {
            match command {
                CacheCommand::StartUp(cache_db) => self.cache = cache_db,
                CacheCommand::StopSentinel => break,
                CacheCommand::Set {
                    key,
                    value,
                    expiry,
                    ttl_sender,
                } => {
                    // Maybe you have to pass sender?

                    let _ = tokio::join!(
                        self.cache
                            .handle_set(key.clone(), value.clone(), expiry, ttl_sender),
                        self.outbox.send(AOFCommand::Set { key, value, expiry })
                    );
                }
                CacheCommand::Get { key, sender } => {
                    self.cache.handle_get(key, sender);
                }
                CacheCommand::Keys { pattern, sender } => {
                    self.cache.handle_keys(pattern, sender);
                }
                CacheCommand::Delete(key) => self.cache.handle_delete(&key),
            }
        }
        Ok(())
    }

    pub fn run_multiple(num_of_actors: usize) -> CacheDbMessageRouter {
        let mut cache_senders = CacheDbMessageRouter::new(num_of_actors);

        (0..num_of_actors).for_each(|actor_id| {
            let sender = CacheActor::run(actor_id);
            cache_senders.push(sender);
        });

        cache_senders
    }
}
