use crate::{
    make_smart_pointer,
    services::{query_io::QueryIO, CacheEntry},
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use super::{save_actor::SaveActorCommand, ttl_actor::TtlInbox};

pub enum CacheCommand {
    Set {
        cache_entry: CacheEntry,
        ttl_sender: TtlInbox,
    },
    Save {
        outbox: mpsc::Sender<SaveActorCommand>,
    },
    Get {
        key: String,
        sender: oneshot::Sender<QueryIO>,
    },
    Keys {
        pattern: Option<String>,
        sender: oneshot::Sender<QueryIO>,
    },
    Delete(String),

    StopSentinel,
}

#[derive(Default)]
pub struct CacheDb(HashMap<String, String>);
impl CacheDb {
    fn keys_stream(&self, pattern: Option<String>) -> impl Iterator<Item = QueryIO> + '_ {
        self.keys().filter_map(move |k| {
            if pattern.as_ref().map_or(true, |p| k.contains(p)) {
                Some(QueryIO::BulkString(k.to_string()))
            } else {
                None
            }
        })
    }
}
pub struct CacheChunk(pub Vec<(String, String)>);
impl CacheChunk {
    pub fn new<'a>(chunk: &'a [(&'a String, &'a String)]) -> Self {
        Self(
            chunk
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<(String, String)>>(),
        )
    }
}

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
                    cache_entry,
                    ttl_sender,
                } => {
                    // Maybe you have to pass sender?

                    match cache_entry {
                        CacheEntry::KeyValue(key, value) => {
                            cache.insert(key, value);
                        }
                        CacheEntry::KeyValueExpiry(key, value, expiry) => {
                            cache.insert(key.clone(), value);
                            ttl_sender.set_ttl(key, expiry.to_u64()).await;
                        }
                    }
                }
                CacheCommand::Get { key, sender } => {
                    let _ = sender.send(cache.get(&key).cloned().into());
                }
                CacheCommand::Keys { pattern, sender } => {
                    let ks = cache.keys_stream(pattern).collect();
                    sender
                        .send(QueryIO::Array(ks))
                        .map_err(|_| anyhow::anyhow!("Error sending keys"))?;
                }
                CacheCommand::Delete(key) => {
                    cache.remove(&key);
                }
                CacheCommand::Save { outbox } => {
                    for chunk in cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox
                            .send(SaveActorCommand::SaveChunk(CacheChunk::new(chunk)))
                            .await?;
                    }
                    // finalize the save operation
                    outbox.send(SaveActorCommand::StopSentinel).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct CacheMessageInbox(tokio::sync::mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheDb, HashMap<String, String>);
make_smart_pointer!(CacheMessageInbox, tokio::sync::mpsc::Sender<CacheCommand>);
