use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::caches::read_queue::ReadQueue;
use crate::domains::cluster_actors::hash_ring::fnv_1a_hash;
use crate::domains::query_parsers::QueryIO;
use crate::make_smart_pointer;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;

pub struct CacheActor {
    pub(crate) cache: CacheDb,
    pub(crate) self_handler: Sender<CacheCommand>,
}

impl CacheActor {
    pub(crate) fn run(hwm: Arc<AtomicU64>) -> CacheCommandSender {
        let (tx, cache_actor_inbox) = mpsc::channel(100);
        tokio::spawn(
            Self { cache: CacheDb::default(), self_handler: tx.clone() }
                .handle(cache_actor_inbox, ReadQueue::new(hwm)),
        );
        CacheCommandSender(tx)
    }

    pub(crate) fn len(&self) -> usize {
        self.cache.len()
    }
    pub(crate) fn keys_with_expiry(&self) -> usize {
        self.cache.keys_with_expiry
    }

    pub(crate) fn keys_stream(
        &self,
        pattern: Option<String>,
    ) -> impl Iterator<Item = QueryIO> + '_ {
        self.cache.keys().filter_map(move |k| {
            if pattern.as_ref().is_none_or(|p| k.contains(p)) {
                Some(QueryIO::BulkString(k.clone()))
            } else {
                None
            }
        })
    }
    pub(crate) fn delete(&mut self, key: String, callback: oneshot::Sender<bool>) {
        if let Some(value) = self.cache.remove(&key) {
            if value.has_expiry() {
                self.cache.keys_with_expiry -= 1;
            }
            let _ = callback.send(true);
        } else {
            let _ = callback.send(false);
        }
    }
    pub(crate) fn exists(&self, key: String, callback: oneshot::Sender<bool>) {
        if self.cache.get(&key).is_some() {
            let _ = callback.send(true);
        } else {
            let _ = callback.send(false);
        }
    }
    pub(crate) fn get(&self, key: &str, callback: oneshot::Sender<Option<CacheValue>>) {
        let _ = callback.send(self.cache.get(key).cloned());
    }

    pub(crate) fn set(&mut self, cache_entry: CacheEntry) {
        let (key, value) = cache_entry.destructure();
        if value.has_expiry() {
            self.cache.keys_with_expiry += 1;
        }
        self.cache.insert(key, value);
    }

    pub(crate) async fn try_send_ttl(&self, cache_entry: &CacheEntry) -> anyhow::Result<()> {
        let Some(expire_in) = cache_entry.expire_in()? else { return Ok(()) };
        let handler = self.self_handler.clone();
        tokio::spawn({
            let key = cache_entry.key().to_string();
            async move {
                tokio::time::sleep(expire_in).await;
                let (tx, rx) = oneshot::channel();
                let _ = handler.send(CacheCommand::Delete { key, callback: tx }).await;
                let _ = rx.await;
            }
        });
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct CacheDb {
    inner: HashMap<String, CacheValue>,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    pub(crate) keys_with_expiry: usize,
}

impl CacheDb {
    /// Extracts (removes) keys and values that fall within the specified token ranges.
    /// This is a mutable operation that modifies the cache by taking out relevant entries.
    ///
    /// # Returns
    /// A HashMap containing the extracted keys and values
    pub(crate) fn extract_keys_for_ranges(
        &mut self,
        token_ranges: Vec<Range<u64>>,
    ) -> HashMap<String, CacheValue> {
        // If no ranges, return empty HashMap
        if token_ranges.is_empty() {
            return HashMap::new();
        }

        // Identify keys that fall within the specified ranges
        let mut keys_to_extract = Vec::new();

        for (key, _value) in self.iter() {
            let key_hash = fnv_1a_hash(key);

            // Check if this key's hash falls within any of the specified ranges
            for range in &token_ranges {
                if key_hash >= range.start && key_hash < range.end {
                    keys_to_extract.push(key.clone());
                    break; // No need to check other ranges once we've found a match
                }
            }
        }

        // Extract the identified keys and values
        let mut extracted = HashMap::new();

        for key in keys_to_extract {
            if let Some(value) = self.remove(&key) {
                // Update the keys_with_expiry counter if this key had an expiry
                if value.expiry.is_some() {
                    self.keys_with_expiry -= 1;
                }

                extracted.insert(key, value);
            }
        }

        extracted
    }
}
#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
make_smart_pointer!(CacheDb, HashMap<String, CacheValue> => inner);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_keys_for_ranges_empty() {
        let mut cache = CacheDb::default();
        let ranges: Vec<Range<u64>> = Vec::new();

        let result = cache.extract_keys_for_ranges(ranges);
        assert!(result.is_empty());
    }
}
