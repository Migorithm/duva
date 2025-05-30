#![allow(dead_code)]

use crate::domains::{caches::cache_objects::CacheValue, cluster_actors::hash_ring::fnv_1a_hash};
use std::{collections::HashMap, marker::PhantomData, ops::Range};
use tracing::error;

pub(crate) struct CacheDb {
    map: HashMap<u64, CacheValue, NoneHasher>,
    key_map: HashMap<String, u64>,
    links: HashMap<u64, (Option<u64>, Option<u64>), NoneHasher>, // (prev, next)
    head: Option<u64>,
    tail: Option<u64>,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    keys_with_expiry: usize,
}
pub(crate) struct Iter<'a> {
    inner: std::collections::hash_map::Iter<'a, String, u64>,
    parent: &'a CacheDb,
}
pub(crate) struct IterMut<'a> {
    inner: std::collections::hash_map::Iter<'a, String, u64>,
    parent: &'a mut CacheDb,
}
pub(crate) struct Values<'a> {
    inner: Iter<'a>,
}
pub(crate) struct ValuesMut<'a> {
    inner: IterMut<'a>,
}
pub(crate) struct Keys<'a> {
    inner: Iter<'a>,
}
pub(crate) struct KeysMut<'a> {
    inner: IterMut<'a>,
}
pub(crate) enum Entry<'a> {
    Occupied { key: String, value: &'a mut CacheValue },
    Vacant { key: String, parent: &'a mut CacheDb },
}
struct NoneHasher;
impl std::hash::BuildHasher for NoneHasher {
    type Hasher = NoHashHasher<u64>;

    fn build_hasher(&self) -> Self::Hasher {
        NoHashHasher(0, PhantomData)
    }
}

// SOURCE: https://crates.io/crates/nohash-hasher
struct NoHashHasher<T>(u64, PhantomData<T>);

impl<T> std::fmt::Debug for NoHashHasher<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_tuple("NoHashHasher").field(&self.0).finish()
    }
}

impl<T> Default for NoHashHasher<T> {
    fn default() -> Self {
        NoHashHasher(0, PhantomData)
    }
}

impl<T> Clone for NoHashHasher<T> {
    fn clone(&self) -> Self {
        NoHashHasher(self.0, self.1)
    }
}

impl<T> Copy for NoHashHasher<T> {}
trait NoHashTarget {}

impl NoHashTarget for u8 {}
impl NoHashTarget for u16 {}
impl NoHashTarget for u32 {}
impl NoHashTarget for u64 {}
impl NoHashTarget for usize {}
impl NoHashTarget for i8 {}
impl NoHashTarget for i16 {}
impl NoHashTarget for i32 {}
impl NoHashTarget for i64 {}
impl NoHashTarget for isize {}
impl<T: NoHashTarget> std::hash::Hasher for NoHashHasher<T> {
    fn write(&mut self, _: &[u8]) {
        panic!("Invalid use of NoHashHasher")
    }

    fn write_u8(&mut self, n: u8) {
        self.0 = u64::from(n)
    }
    fn write_u16(&mut self, n: u16) {
        self.0 = u64::from(n)
    }
    fn write_u32(&mut self, n: u32) {
        self.0 = u64::from(n)
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n
    }
    fn write_usize(&mut self, n: usize) {
        self.0 = n as u64
    }

    fn write_i8(&mut self, n: i8) {
        self.0 = n as u64
    }
    fn write_i16(&mut self, n: i16) {
        self.0 = n as u64
    }
    fn write_i32(&mut self, n: i32) {
        self.0 = n as u64
    }
    fn write_i64(&mut self, n: i64) {
        self.0 = n as u64
    }
    fn write_isize(&mut self, n: isize) {
        self.0 = n as u64
    }

    fn finish(&self) -> u64 {
        self.0
    }
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
        let mut hashes_to_extract = Vec::new();

        for key_hash in self.map.keys() {
            // Check if this key's hash falls within any of the specified ranges
            for range in &token_ranges {
                if range.contains(&key_hash) {
                    hashes_to_extract.push(*key_hash);
                    break; // No need to check other ranges once we've found a match
                }
            }
        }

        // Extract the identified keys and values into a fresh CacheDb
        let mut extracted_map = HashMap::with_hasher(NoneHasher);
        let mut extracted_key_map = HashMap::new();
        let mut extracted_expiry_count = 0;
        let head = hashes_to_extract.first().cloned();
        let tail = hashes_to_extract.last().cloned();
        let links = hashes_to_extract.iter().enumerate().fold(
            HashMap::with_hasher(NoneHasher),
            |mut acc, (i, &hash)| {
                let prev = if i == 0 { None } else { Some(hashes_to_extract[i - 1]) };
                let next = if i == hashes_to_extract.len() - 1 {
                    None
                } else {
                    Some(hashes_to_extract[i + 1])
                };
                acc.insert(hash, (prev, next));
                acc
            },
        );
        for hash in hashes_to_extract {
            let Some(value) = self.map.remove(&hash) else {
                error!("CacheDb: Key not found in map");
                continue;
            };
            let Some(key) = self
                .key_map
                .iter()
                .find_map(|(k, &v)| if v == hash { Some(k.clone()) } else { None })
            else {
                error!("CacheDb: Key not found in key_map for the given hash");
                continue;
            };
            self.key_map.remove(&key);
            if value.has_expiry() {
                extracted_expiry_count += 1;
                self.keys_with_expiry -= 1;
            }
            extracted_map.insert(hash, value);
            extracted_key_map.insert(key, hash);
        }
        CacheDb {
            map: extracted_map,
            key_map: extracted_key_map,
            links,
            head,
            tail,
            keys_with_expiry: extracted_expiry_count,
        }
    }
    pub(crate) fn keys_with_expiry(&self) -> usize {
        self.keys_with_expiry
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }
    #[inline]
    pub fn is_exist(&self, key: &str) -> bool {
        self.key_map.contains_key(key)
    }
    #[inline]
    pub fn get(&self, key: &str) -> Option<&CacheValue> {
        // SAFETY: mut needed by refreshing LRU order. it won't change inner data
        #[allow(mutable_transmutes)]
        unsafe { std::mem::transmute::<&Self, &mut Self>(self).get_mut(key) }
            .map(|v| v as &CacheValue)
    }
    #[inline]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut CacheValue> {
        if let Some(&id) = self.key_map.get(key) {
            self.move_to_head(id);
            self.map.get_mut(&id)
        } else {
            None
        }
    }
    #[inline]
    pub fn insert(&mut self, key: String, value: CacheValue) {
        if let Some(&existing_id) = self.key_map.get(&key) {
            self.map.insert(existing_id, value);
            self.move_to_head(existing_id);
        } else {
            // Detect empty key
            let mut id = fnv_1a_hash(&key);
            while self.map.contains_key(&id) {
                id = id.wrapping_add(1);
            }
            if value.has_expiry() {
                self.keys_with_expiry += 1;
            }
            self.map.insert(id, value.clone());
            self.key_map.insert(key, id);
            self.push_front(id);
        }
    }
    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<CacheValue> {
        if let Some(&id) = self.key_map.get(key) {
            let val = self.map.remove(&id)?;
            self.detach(id);
            self.key_map.remove(key); // 정확히 하나만 제거
            if val.has_expiry() {
                self.keys_with_expiry -= 1;
            }
            Some(val)
        } else {
            None
        }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.key_map.contains_key(key)
    }

    // LRU list helpers
    fn move_to_head(&mut self, id: u64) {
        self.detach(id);
        self.push_front(id);
    }
    #[inline]
    fn detach(&mut self, id: u64) {
        if let Some((prev, next)) = self.links.remove(&id) {
            if let Some(p) = prev {
                if let Some(entry) = self.links.get_mut(&p) {
                    entry.1 = next;
                }
            } else {
                self.head = next;
            }
            if let Some(n) = next {
                if let Some(entry) = self.links.get_mut(&n) {
                    entry.0 = prev;
                }
            } else {
                self.tail = prev;
            }
        }
    }
    #[inline]
    fn push_front(&mut self, id: u64) {
        let old_head = self.head;
        self.links.insert(id, (None, old_head));
        if let Some(h) = old_head {
            if let Some(entry) = self.links.get_mut(&h) {
                entry.0 = Some(id);
            }
        } else {
            self.tail = Some(id);
        }
        self.head = Some(id);
    }
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter { inner: self.key_map.iter(), parent: self }
    }
    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_> {
        let parent = self as *mut CacheDb;
        // SAFETY: to make lifetime work
        let parent = unsafe { &mut *parent };
        let inner = self.key_map.iter();
        IterMut { inner, parent }
    }
    #[inline]
    pub fn entry(&mut self, key: String) -> Entry<'_> {
        if let Some(&id) = self.key_map.get(&key) {
            self.move_to_head(id);
            Entry::Occupied { key, value: self.map.get_mut(&id).unwrap() }
        } else {
            Entry::Vacant { key, parent: self }
        }
    }
    #[inline]
    pub fn values(&self) -> Values<'_> {
        Values { inner: self.iter() }
    }
    #[inline]
    pub fn values_mut(&mut self) -> ValuesMut<'_> {
        ValuesMut { inner: self.iter_mut() }
    }
    #[inline]
    pub fn keys(&self) -> Keys<'_> {
        Keys { inner: self.iter() }
    }
    #[inline]
    pub fn keys_mut(&mut self) -> KeysMut<'_> {
        KeysMut { inner: self.iter_mut() }
    }
    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
        self.key_map.clear();
        self.links.clear();
        self.head = None;
        self.tail = None;
        self.keys_with_expiry = 0;
    }
}
impl<'a> Entry<'a> {
    #[inline]
    fn insert_parent(parent: &'a mut CacheDb, k: String, v: CacheValue) -> &'a mut CacheValue {
        let id = if let Some(&existing_id) = parent.key_map.get(&k) {
            parent.map.insert(existing_id, v);
            parent.move_to_head(existing_id);
            existing_id
        } else {
            // Detect empty key
            let mut hash = fnv_1a_hash(&k);
            while parent.map.contains_key(&hash) {
                hash = hash.wrapping_add(1);
            }
            if v.has_expiry() {
                parent.keys_with_expiry += 1;
            }
            parent.map.insert(hash, v.clone());
            parent.key_map.insert(k.clone(), hash);
            parent.push_front(hash);
            hash
        };
        parent.map.get_mut(&id).expect("Key should exist after insertion")
    }
    #[inline]
    pub fn key(&self) -> &String {
        match self {
            | Entry::Occupied { key, .. } | Entry::Vacant { key, .. } => key,
        }
    }

    #[inline]
    pub fn or_insert(self, default: CacheValue) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => value,
            | Entry::Vacant { key, parent } => Self::insert_parent(parent, key, default),
        }
    }

    #[inline]
    pub fn or_insert_with<F: FnOnce() -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => value,
            | Entry::Vacant { key, parent } => {
                let val = f();
                Self::insert_parent(parent, key, val)
            },
        }
    }

    #[inline]
    pub fn or_insert_with_key<F: FnOnce(&String) -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => value,
            | Entry::Vacant { key, parent } => {
                let val = f(&key);
                Self::insert_parent(parent, key, val)
            },
        }
    }

    #[inline]
    pub fn and_modify<F: FnOnce(&mut CacheValue)>(self, f: F) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, value } => {
                f(value);
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => Entry::Vacant { key, parent },
        }
    }

    #[inline]
    pub fn insert_entry(self, new_val: CacheValue) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, value } => {
                *value = new_val;
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => {
                let v = Self::insert_parent(parent, key.clone(), new_val);
                Entry::Occupied { key, value: v }
            },
        }
    }
}
impl Default for CacheDb {
    fn default() -> Self {
        CacheDb {
            map: HashMap::with_hasher(NoneHasher),
            key_map: HashMap::new(),
            links: HashMap::with_hasher(NoneHasher),
            head: None,
            tail: None,
            keys_with_expiry: 0,
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a String, &'a CacheValue);
    fn next(&mut self) -> Option<Self::Item> {
        let (key, &id) = self.inner.next()?;
        self.parent.map.get(&id).map(|v| (key, v))
    }
}

impl<'a> Iterator for IterMut<'a> {
    type Item = (&'a String, &'a mut CacheValue);
    fn next(&mut self) -> Option<Self::Item> {
        let (key, &id) = self.inner.next()?;
        let ptr = self.parent as *mut CacheDb;
        // SAFETY: to make lifetime work
        unsafe { (*ptr).map.get_mut(&id).map(|v| (key, v)) }
    }
}

impl<'a> Iterator for Values<'a> {
    type Item = &'a CacheValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a> Iterator for ValuesMut<'a> {
    type Item = &'a mut CacheValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a> Iterator for Keys<'a> {
    type Item = &'a String;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k)
    }
}

impl<'a> Iterator for KeysMut<'a> {
    type Item = &'a String;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k)
    }
}

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
        cache.insert("key1".to_string(), CacheValue::new("value".to_string()));

        // Create a range that doesn't include our key's hash
        let key_hash = fnv_1a_hash("key1");
        let ranges = vec![(key_hash + 1000)..u64::MAX];

        let result = cache.take_subset(ranges);
        assert!(result.is_empty());
        assert_eq!(cache.len(), 1); // Cache still has our key
    }

    #[test]
    fn test_extract_keys_for_ranges_with_matches() {
        let mut cache = CacheDb::default();

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
        let extracted = cache.take_subset(ranges);

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
    }
    #[test]
    fn test_none_hasher() {
        let hasher = NoneHasher;
        for _ in 0..100 {
            let now = rand::random::<u64>();
            let h = std::hash::BuildHasher::hash_one(&hasher, now);
            assert_eq!(h, now);
        }
    }
}
