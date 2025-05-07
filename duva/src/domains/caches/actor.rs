use super::cache_objects::{CacheEntry, CacheValue};
use super::command::CacheCommand;
use crate::domains::caches::read_queue::ReadQueue;
use crate::domains::cluster_actors::hash_ring::fnv_1a_hash;
use crate::domains::query_parsers::QueryIO;
use crate::make_smart_pointer;

use anyhow::Context;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
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

    pub(crate) fn keys(&self, pattern: Option<String>, callback: oneshot::Sender<QueryIO>) {
        let keys = self
            .cache
            .keys()
            .filter_map(move |k| {
                if pattern.as_ref().is_none_or(|p| k.contains(p)) {
                    Some(QueryIO::BulkString(k.clone()))
                } else {
                    None
                }
            })
            .collect();
        let _ = callback.send(QueryIO::Array(keys));
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
        let _ = callback.send(self.cache.get(&key).is_some());
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

    pub(crate) fn append(
        &mut self,
        key: String,
        value: String,
        callback: oneshot::Sender<anyhow::Result<usize>>,
    ) {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert(CacheValue { value: "".to_string(), expiry: None });
        val.value.push_str(value.as_str());

        let _ = callback.send(Ok(val.value.len()));
    }

    pub(crate) fn numeric_delta(
        &mut self,
        key: String,
        delta: i64,
        callback: oneshot::Sender<anyhow::Result<i64>>,
    ) {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert(CacheValue { value: "0".to_string(), expiry: None });

        let Ok(curr) = val.value.parse::<i64>() else {
            let _ =
                callback.send(Err(anyhow::anyhow!("ERR value is not an integer or out of range")));
            return;
        };

        let _ = callback.send(Ok(curr + delta));
        val.value = (curr + delta).to_string();
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
    pub(crate) fn take_subset(&mut self, token_ranges: Vec<Range<u64>>) -> CacheDb {
        // If no ranges, return empty HashMap
        if token_ranges.is_empty() {
            return CacheDb::default();
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
        let mut extracted_keys_with_expiry = 0;
        for key in keys_to_extract {
            if let Some(value) = self.remove(&key) {
                // Update the keys_with_expiry counter if this key had an expiry
                if value.expiry.is_some() {
                    self.keys_with_expiry -= 1;
                    extracted_keys_with_expiry += 1;
                }

                extracted.insert(key, value);
            }
        }
        CacheDb { inner: extracted, keys_with_expiry: extracted_keys_with_expiry }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct CacheCommandSender(pub(crate) mpsc::Sender<CacheCommand>);

make_smart_pointer!(CacheCommandSender, mpsc::Sender<CacheCommand>);
make_smart_pointer!(CacheDb, HashMap<String, CacheValue> => inner);

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    fn test_extract_keys_for_ranges_empty() {
        let mut cache = CacheDb::default();
        let ranges: Vec<Range<u64>> = Vec::new();

        let result = cache.take_subset(ranges);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_keys_for_ranges_no_matches() {
        let mut cache = CacheDb::default();
        cache.inner.insert("key1".to_string(), CacheValue::new("value".to_string()));

        // Create a range that doesn't include our key's hash
        let key_hash = fnv_1a_hash("key1");
        let ranges = vec![(key_hash + 1000)..u64::MAX];

        let result = cache.take_subset(ranges);
        assert!(result.is_empty());
        assert_eq!(cache.inner.len(), 1); // Cache still has our key
    }

    #[test]
    fn test_extract_keys_for_ranges_with_matches() {
        let mut cache = CacheDb::default();

        // Add several keys to the cache
        for i in 0..10 {
            let key = format!("key{}", i);
            let has_expiry = i % 2 == 0;

            cache.inner.insert(
                key,
                CacheValue::new(format!("value{}", i)).with_expiry(if has_expiry {
                    Some(Utc::now())
                } else {
                    None
                }),
            );

            if has_expiry {
                cache.keys_with_expiry += 1;
            }
        }

        assert_eq!(cache.inner.len(), 10);
        assert_eq!(cache.keys_with_expiry, 5); // Keys 0, 2, 4, 6, 8 have expiry

        // Create ranges to extract specific keys (using their hash values)
        let mut ranges = Vec::new();
        for i in 0..5 {
            let key = format!("key{}", i);
            let key_hash = fnv_1a_hash(&key);
            ranges.push(key_hash..key_hash + 1); // Range that contains just this key's hash
        }

        // Extract keys 0-4
        let extracted = cache.take_subset(ranges);

        // Verify extraction
        assert_eq!(extracted.len(), 5);
        for i in 0..5 {
            let key = format!("key{}", i);
            assert!(extracted.contains_key(&key));
            assert!(!cache.inner.contains_key(&key));
        }

        // Verify remaining cache
        assert_eq!(cache.inner.len(), 5);
        for i in 5..10 {
            let key = format!("key{}", i);
            assert!(cache.inner.contains_key(&key));
        }

        // Verify keys_with_expiry counter was updated correctly
        // We should have removed keys 0, 2, 4 with expiry, so count should be reduced by 3
        assert_eq!(cache.keys_with_expiry, 2); // Only keys 6, 8 remain with expiry
        assert_eq!(extracted.keys_with_expiry, 3); // Keys 0, 2, 4 had expiry
    }
}
