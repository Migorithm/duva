use crate::domains::cache::actor::CacheActor;
use crate::domains::cache::cache_objects::CacheEntry;
use crate::domains::cache::command::CacheCommand;

use crate::services::query_io::QueryIO;
use crate::services::statefuls::snapshot::save::command::SaveCommand;
use anyhow::Result;

impl CacheActor {
    pub async fn handle(mut self) -> Result<Self> {
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
                        .send(SaveCommand::LocalShardSize {
                            table_size: self.len(),
                            expiry_size: self.keys_with_expiry(),
                        })
                        .await?;
                    for chunk in self.cache.iter().collect::<Vec<_>>().chunks(10) {
                        outbox.send(SaveCommand::SaveChunk(CacheEntry::new(chunk))).await?;
                    }
                    // finalize the save operation
                    outbox.send(SaveCommand::StopSentinel).await?;
                }
            }
        }
        Ok(self)
    }
}

#[tokio::test]
async fn test_set_and_delete_inc_dec_keys_with_expiry() {
    use crate::domains::cache::actor::CacheDb;
    use crate::domains::ttl::manager::TtlSchedulerManager;
    use std::time::{Duration, SystemTime};

    // GIVEN
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let actor = CacheActor { inbox: rx, cache: CacheDb::default() };

    let (ttl_tx, mut ttx_rx) = tokio::sync::mpsc::channel(100);
    let ttl_sender = TtlSchedulerManager(ttl_tx);
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
