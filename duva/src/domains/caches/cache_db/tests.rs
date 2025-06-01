use super::*;
use crate::domains::cluster_actors::hash_ring::fnv_1a_hash;
use chrono::Utc;

#[test]
fn test_extract_keys_for_ranges_empty() {
    let mut cache = CacheDb::with_capacity(100);
    let ranges: Vec<Range<u64>> = Vec::new();

    let result = cache.copy_subset_per_token_ranges(ranges);
    assert!(result.is_empty());
}

#[test]
fn test_extract_keys_for_ranges_no_matches() {
    let mut cache = CacheDb::with_capacity(100);
    cache.insert("key1".to_string(), CacheValue::new("value".to_string()));

    // Create a range that doesn't include our key's hash
    let key_hash = fnv_1a_hash("key1");
    let ranges = vec![(key_hash + 1000)..u64::MAX];

    let result = cache.copy_subset_per_token_ranges(ranges);
    assert!(result.is_empty());
    assert_eq!(cache.len(), 1); // Cache still has our key

    assert!(cache.validate(), "CacheDb validation failed");
}

#[test]
fn test_extract_keys_for_ranges_with_matches() {
    let mut cache = CacheDb::with_capacity(100);

    // Add several keys to the cache
    for i in 0..10 {
        let key = format!("key{}", i);
        let has_expiry = i % 2 == 0;

        cache.insert(
            key,
            CacheValue::new(format!("value{}", i)).with_expiry(if has_expiry {
                Some(Utc::now())
            } else {
                None
            }),
        );
    }

    assert_eq!(cache.len(), 10);
    assert_eq!(cache.keys_with_expiry(), 5); // Keys 0, 2, 4, 6, 8 have expiry

    // Create ranges to extract specific keys (using their hash values)
    let mut ranges = Vec::new();
    for i in 0..5 {
        let key = format!("key{}", i);
        let key_hash = fnv_1a_hash(&key);
        ranges.push(key_hash..key_hash + 1); // Range that contains just this key's hash
    }

    // Extract keys 0-4
    let extracted = cache.copy_subset_per_token_ranges(ranges);

    // Verify extraction
    assert_eq!(extracted.len(), 5);
    for i in 0..5 {
        let key = format!("key{}", i);
        assert!(extracted.contains_key(&key));
        assert!(!cache.contains_key(&key));
    }

    // Verify remaining cache
    assert_eq!(cache.len(), 5);
    for i in 5..10 {
        let key = format!("key{}", i);
        assert!(cache.contains_key(&key));
    }

    // Verify keys_with_expiry counter was updated correctly
    // We should have removed keys 0, 2, 4 with expiry, so count should be reduced by 3
    assert_eq!(cache.keys_with_expiry, 2); // Only keys 6, 8 remain with expiry
    assert_eq!(extracted.keys_with_expiry, 3); // Keys 0, 2, 4 had expiry

    assert!(cache.validate(), "CacheDb validation failed");
    assert!(extracted.validate(), "Extracted CacheDb validation failed");
}
#[test]
fn test_lru_with_expiry() {
    let mut cache = CacheDb::with_capacity(3);
    let expiry = Some(Utc::now() + chrono::Duration::minutes(10));
    assert_eq!(cache.capacity, 3);

    for i in 0..3 {
        cache.insert(
            format!("key{}", i),
            CacheValue::new(format!("value{}", i)).with_expiry(expiry),
        );
    }
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.keys_with_expiry(), 3);

    cache.insert("key999".to_string(), CacheValue::new("value3".to_string()).with_expiry(expiry));
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.keys_with_expiry(), 3);

    cache
        .entry("key998".to_string())
        .or_insert_with(|| CacheValue::new("value4".to_string()).with_expiry(expiry));
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.keys_with_expiry(), 3);

    assert!(cache.is_exist("key2"));

    assert!(cache.validate(), "CacheDb validation failed");
}
#[test]
fn test_get_insert_remove_entry() {
    let mut cache = CacheDb::with_capacity(100);
    let expiry = Some(Utc::now() + chrono::Duration::minutes(10));

    assert!(cache.lookup("key1").is_none());

    cache.insert("key1".to_string(), CacheValue::new("value1".to_string()).with_expiry(expiry));
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.keys_with_expiry(), 1);
    assert!(cache.lookup("key1").is_some());

    cache.insert("key1".to_string(), CacheValue::new("value2".to_string()).with_expiry(expiry));
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.keys_with_expiry(), 1);

    cache.insert("key2".to_string(), CacheValue::new("value3".to_string()).with_expiry(expiry));
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.keys_with_expiry(), 2);

    assert!(cache.remove("key1").is_some());
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.keys_with_expiry(), 1);

    for i in 0..100 {
        cache.insert(
            format!("key{}", i),
            CacheValue::new(format!("value{}", i)).with_expiry(expiry),
        );
    }
    assert_eq!(cache.len(), 100);
    assert_eq!(cache.keys_with_expiry(), 100);

    let mut count = 0;
    cache.iter().for_each(|_| count += 1);
    assert_eq!(count, 100);

    cache.entry("key50".to_string()).and_modify(|v| {
        v.value = "modified_value".to_string();
    });
    assert_eq!(cache.lookup("key50").unwrap().value(), "modified_value");

    cache.entry("key50".to_string()).or_insert(CacheValue::new("new_value".to_string()));
    assert_eq!(cache.lookup("key50").unwrap().value(), "modified_value");

    cache.entry("key999".to_string()).and_modify(|v| {
        v.value = "modified_value".to_string();
    });
    assert!(cache.lookup("key999").is_none());

    assert!(cache.validate(), "CacheDb validation failed");

    assert_eq!(cache.len(), 100);
    assert_eq!(cache.keys_with_expiry(), 100);
    cache.clear();
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.keys_with_expiry(), 0);
}
