use crate::{
    make_smart_pointer,
    services::{statefuls::ttl_handlers::set::TtlInbox, value::Value},
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use super::save_actor::SaveActorCommand;

pub enum CacheCommand {
    Set {
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlInbox,
    },
    Save {
        outbox: mpsc::Sender<SaveActorCommand>,
    },
    Get {
        key: String,
        sender: oneshot::Sender<Value>,
    },
    Keys {
        pattern: Option<String>,
        sender: oneshot::Sender<Value>,
    },
    Delete(String),
    StopSentinel,
}

#[derive(Default)]
pub struct CacheDb(HashMap<String, String>);

impl CacheDb {
    pub async fn handle_set(
        &mut self,
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlInbox,
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
    inbox: tokio::sync::mpsc::Receiver<CacheCommand>,
}
impl CacheActor {
    // Create a new CacheActor with inner state
    pub fn run() -> CacheMessageInbox {
        let (tx, cache_actor_inbox) = tokio::sync::mpsc::channel(100);
        tokio::spawn(
            Self {
                inbox: cache_actor_inbox,
            }
            .handle(),
        );
        CacheMessageInbox(tx)
    }

    async fn handle(mut self) -> Result<()> {
        let mut cache = CacheDb::default();

        while let Some(command) = self.inbox.recv().await {
            match command {
                CacheCommand::StopSentinel => break,
                CacheCommand::Set {
                    key,
                    value,
                    expiry,
                    ttl_sender,
                } => {
                    // Maybe you have to pass sender?
                    let _ = cache
                        .handle_set(key.clone(), value.clone(), expiry, ttl_sender)
                        .await;
                }
                CacheCommand::Get { key, sender } => {
                    cache.handle_get(key, sender);
                }
                CacheCommand::Keys { pattern, sender } => {
                    cache.handle_keys(pattern, sender);
                }
                CacheCommand::Delete(key) => cache.handle_delete(&key),
                CacheCommand::Save { outbox } => {
                    for chunk in cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox
                            .send(SaveActorCommand::SaveChunk(
                                chunk
                                    .iter()
                                    .map(|(k, v)| ((*k).clone(), (*v).clone()))
                                    .collect::<Vec<(String, String)>>(),
                            ))
                            .await?;
                    }
                    outbox.send(SaveActorCommand::StopSentinel).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct CacheMessageInbox(tokio::sync::mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheMessageInbox, tokio::sync::mpsc::Sender<CacheCommand>);
