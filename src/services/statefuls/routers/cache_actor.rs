use super::{aof_actor::AOFActor, inmemory_router::CacheDispatcher};
use crate::{
    make_smart_pointer,
    services::{
        statefuls::{
            command::{AOFCommand, CacheCommand},
            ttl_handlers::set::TtlHandler,
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
        ttl_sender: TtlHandler,
    ) -> Result<Value> {
        match expiry {
            Some(expiry) => {
                self.insert(key.clone(), value);
                ttl_sender.set_ttl(key, expiry).await;
            }
            None => {
                self.insert(key, value);
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
            .filter_map(|k| {
                if pattern.as_ref().map_or(true, |p| k.contains(p)) {
                    Some(Value::BulkString(k.clone()))
                } else {
                    None
                }
            })
            .collect();
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
    fn run(actor_id: usize) -> CacheHandler {
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
        CacheHandler(tx)
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

    pub fn run_multiple(num_of_actors: usize) -> CacheDispatcher {
        (0..num_of_actors)
            .into_iter()
            .map(|actor_id| CacheActor::run(actor_id))
            .collect::<Vec<_>>()
            .into()
    }
}

#[derive(Clone)]
pub struct CacheHandler(tokio::sync::mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheHandler, tokio::sync::mpsc::Sender<CacheCommand>);
