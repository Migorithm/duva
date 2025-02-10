use super::CacheEntry;
use super::CacheValue;
use crate::make_smart_pointer;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use crate::services::statefuls::snapshot::encoding_command::EncodingCommand;
use anyhow::Context;
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub enum CacheCommand {
    Set { cache_entry: CacheEntry, ttl_sender: TtlSchedulerInbox },
    Save { outbox: mpsc::Sender<EncodingCommand> },
    Get { key: String, sender: oneshot::Sender<QueryIO> },
    Keys { pattern: Option<String>, sender: oneshot::Sender<QueryIO> },
    Delete(String),
    StopSentinel,
}

#[derive(Default)]
pub struct CacheDb {
    inner: HashMap<String, CacheValue>,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    pub(crate) keys_with_expiry: usize,
}

pub struct CacheActor {
    inbox: mpsc::Receiver<CacheCommand>,
    cache: CacheDb,
}
impl CacheActor {
    // Create a new CacheActor with inner state
    pub fn run() -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(Self { inbox: cache_actor_inbox, cache: CacheDb::default() }.handle());
        CacheCommandSender(tx)
    }

    async fn handle(mut self) -> Result<Self> {
        while let Some(command) = self.inbox.recv().await {
            match command {
                CacheCommand::StopSentinel => break,

                CacheCommand::Set { cache_entry, ttl_sender } => {
                    let _ = self
                        .try_send_ttl(cache_entry.key(), cache_entry.expiry(), ttl_sender.clone())
                        .await;
                    self.set(cache_entry);
                }
                CacheCommand::Get { key, sender } => {
                    let _ = sender.send(self.get(&key).into());
                }
                CacheCommand::Keys { pattern, sender } => {
                    let ks: Vec<_> = self.keys_stream(pattern).collect();

                    sender
                        .send(QueryIO::Array(ks))
                        .map_err(|_| anyhow::anyhow!("Error sending keys"))?;
                }
                CacheCommand::Delete(key) => {
                    self.delete(&key);
                }
                CacheCommand::Save { outbox } => {
                    outbox
                        .send(EncodingCommand::LocalShardSize {
                            table_size: self.len(),
                            expiry_size: self.keys_with_expiry(),
                        })
                        .await?;
                    for chunk in self.cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox.send(EncodingCommand::SaveChunk(CacheEntry::new(chunk))).await?;
                    }
                    // finalize the save operation
                    outbox.send(EncodingCommand::StopSentinel).await?;
                }
            }
        }
        Ok(self)
    }

    fn len(&self) -> usize {
        self.cache.len()
    }
    fn keys_with_expiry(&self) -> usize {
        self.cache.keys_with_expiry
    }

    fn keys_stream(&self, pattern: Option<String>) -> impl Iterator<Item = QueryIO> + '_ {
        self.cache.keys().filter_map(move |k| {
            if pattern.as_ref().map_or(true, |p| k.contains(p)) {
                Some(QueryIO::BulkString(k.clone().into()))
            } else {
                None
            }
        })
    }
    fn delete(&mut self, key: &str) {
        if let Some(value) = self.cache.remove(key) {
            if value.has_expiry() {
                self.cache.keys_with_expiry -= 1;
            }
        }
    }
    fn get(&self, key: &str) -> Option<CacheValue> {
        self.cache.get(key).cloned()
    }

    fn set(&mut self, cache_entry: CacheEntry) {
        match cache_entry {
            CacheEntry::KeyValue(key, value) => {
                self.cache.insert(key, CacheValue::Value(value));
            }
            CacheEntry::KeyValueExpiry(key, value, expiry) => {
                self.cache.keys_with_expiry += 1;
                self.cache.insert(key.clone(), CacheValue::ValueWithExpiry(value, expiry));
            }
        }
    }

    async fn try_send_ttl(
        &self,
        key: &str,
        expiry: Option<std::time::SystemTime>,
        ttl_sender: TtlSchedulerInbox,
    ) -> Result<()> {
        ttl_sender.set_ttl(key.to_string(), expiry.context("Expiry not found")?).await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct CacheCommandSender(mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
make_smart_pointer!(CacheDb, HashMap<String, CacheValue> => inner);

#[tokio::test]
async fn test_set_and_delete_inc_dec_keys_with_expiry() {
    use std::time::{Duration, SystemTime};

    // GIVEN
    let (tx, rx) = mpsc::channel(100);
    let actor = CacheActor { inbox: rx, cache: CacheDb::default() };

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
