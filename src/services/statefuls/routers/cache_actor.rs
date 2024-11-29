use crate::{
    make_smart_pointer,
    services::{query_manager::query_io::QueryIO, CacheEntry, CacheValue},
};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use super::{save_actor::SaveActorCommand, ttl_manager::TtlSchedulerInbox};

pub enum CacheCommand {
    Set {
        cache_entry: CacheEntry,
        ttl_sender: TtlSchedulerInbox,
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
pub struct CacheDb {
    inner: HashMap<String, CacheValue>,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    pub(crate) keys_with_expiry: usize,
}
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

    fn count_values_with_expiry(&self) -> usize {
        self.values().filter(|v| v.has_expiry()).count()
    }

    fn set(&mut self, key: String, value: CacheValue) {
        if let CacheValue::ValueWithExpiry(_, _) = value {
            self.keys_with_expiry += 1;
        }
        self.insert(key, value);
    }
    fn delete(&mut self, key: &str) {
        if let Some(value) = self.remove(key) {
            if value.has_expiry() {
                self.keys_with_expiry -= 1;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheChunk(pub Vec<(String, CacheValue)>);
impl CacheChunk {
    pub fn new(chunk: &[(&String, &CacheValue)]) -> Self {
        Self(
            chunk
                .iter()
                .map(|(k, v)| (k.to_string(), (*v).clone()))
                .collect::<Vec<(String, CacheValue)>>(),
        )
    }
}

pub struct CacheActor {
    inbox: mpsc::Receiver<CacheCommand>,
    cache: CacheDb,
}
impl CacheActor {
    // Create a new CacheActor with inner state
    pub fn run() -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(
            Self {
                inbox: cache_actor_inbox,
                cache: CacheDb::default(),
            }
            .handle(),
        );
        CacheCommandSender(tx)
    }

    async fn handle(mut self) -> Result<Self> {
        while let Some(command) = self.inbox.recv().await {
            match command {
                CacheCommand::StopSentinel => break,

                CacheCommand::Set {
                    cache_entry,
                    ttl_sender,
                } => match cache_entry {
                    CacheEntry::KeyValue(key, value) => {
                        self.cache.set(key, CacheValue::Value(value));
                    }
                    CacheEntry::KeyValueExpiry(key, value, expiry) => {
                        self.cache
                            .set(key.clone(), CacheValue::ValueWithExpiry(value, expiry));
                        ttl_sender.set_ttl(key, expiry).await;
                    }
                },
                CacheCommand::Get { key, sender } => {
                    let _ = sender.send(self.cache.get(&key).cloned().into());
                }
                CacheCommand::Keys { pattern, sender } => {
                    let ks = self.cache.keys_stream(pattern).collect();
                    sender
                        .send(QueryIO::Array(ks))
                        .map_err(|_| anyhow::anyhow!("Error sending keys"))?;
                }
                CacheCommand::Delete(key) => {
                    self.cache.delete(&key);
                }
                CacheCommand::Save { outbox } => {
                    outbox
                        .send(SaveActorCommand::LocalShardSize {
                            total_size: self.cache.len(),
                            expiry_size: self.cache.count_values_with_expiry(),
                        })
                        .await?;
                    for chunk in self.cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox
                            .send(SaveActorCommand::SaveChunk(CacheChunk::new(chunk)))
                            .await?;
                    }
                    // finalize the save operation
                    outbox.send(SaveActorCommand::StopSentinel).await?;
                }
            }
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct CacheCommandSender(mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
impl std::ops::Deref for CacheDb {
    type Target = HashMap<String, CacheValue>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl std::ops::DerefMut for CacheDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[tokio::test]
async fn test_set_and_delete_inc_dec_keys_with_expiry() {
    use std::time::{Duration, SystemTime};

    // GIVEN
    let (tx, rx) = mpsc::channel(100);
    let actor = CacheActor {
        inbox: rx,
        cache: CacheDb::default(),
    };

    let (ttl_tx, mut ttx_rx) = mpsc::channel(100);
    let ttl_sender = TtlSchedulerInbox(ttl_tx);
    tokio::spawn(async move { ttx_rx.recv().await });

    // WHEN
    let handler = tokio::spawn(actor.handle());

    
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        tx.send(CacheCommand::Set {
            cache_entry: if i & 1 == 0 {
                CacheEntry::KeyValueExpiry(key, value, SystemTime::now() + Duration::from_secs(10))
            } else {
                CacheEntry::KeyValue(key, value)
            },
            ttl_sender: ttl_sender.clone(),
        })
        .await
        .unwrap();
    }

    // key0 is expiry key. deleting the following will decrese the number by 1
    let delete_key = "key0".to_string();
    tx.send(CacheCommand::Delete(delete_key)).await.unwrap();
    tx.send(CacheCommand::StopSentinel).await.unwrap();
    let actor: CacheActor = handler.await.unwrap().unwrap();

    // THEN
    assert_eq!(actor.cache.keys_with_expiry, 49);
}
