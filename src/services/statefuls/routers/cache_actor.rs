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
                    match expiry {
                        Some(expiry) => {
                            cache.insert(key.clone(), value);
                            ttl_sender.set_ttl(key, expiry).await;
                        }
                        None => {
                            cache.insert(key, value);
                        }
                    }
                }
                CacheCommand::Get { key, sender } => {
                    let _ = sender.send(cache.get(&key).cloned().into());
                }
                CacheCommand::Keys { pattern, sender } => {
                    let ks = cache
                        .keys()
                        .filter_map(|k| {
                            if pattern.as_ref().map_or(true, |p| k.contains(p)) {
                                Some(Value::BulkString(k.clone()))
                            } else {
                                None
                            }
                        })
                        .collect();

                    sender
                        .send(Value::Array(ks))
                        .map_err(|_| anyhow::anyhow!("Error sending keys"))?;
                }
                CacheCommand::Delete(key) => {
                    cache.remove(&key);
                }
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
