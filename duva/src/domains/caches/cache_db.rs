use crate::{
    domains::{caches::cache_objects::CacheValue, cluster_actors::hash_ring::fnv_1a_hash},
    make_smart_pointer,
};
use std::{collections::HashMap, ops::Range};

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
                if range.contains(&key_hash) {
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

make_smart_pointer!(CacheDb, HashMap<String, CacheValue> => inner);

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::domains::cluster_actors::hash_ring::fnv_1a_hash;

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
