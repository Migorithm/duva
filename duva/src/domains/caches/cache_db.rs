#![allow(dead_code)]
#![allow(mutable_transmutes)]

use crate::domains::{caches::cache_objects::CacheValue, cluster_actors::hash_ring::fnv_1a_hash};
use std::{cell::UnsafeCell, collections::HashMap, ops::Range, pin::Pin};
use tracing::error;

pub(crate) struct CacheDb {
    map: HashMap<String, Pin<Box<UnsafeCell<CacheDbNode>>>>,
    head: Option<*mut CacheDbNode>,
    tail: Option<*mut CacheDbNode>,
    capacity: usize,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    keys_with_expiry: usize,
}
#[derive(Clone)]
struct CacheDbNode {
    key: String,
    value: Box<CacheValue>,
    prev: Option<*mut CacheDbNode>,
    next: Option<*mut CacheDbNode>,
}
unsafe impl Send for CacheDb {}
unsafe impl Sync for CacheDb {}
pub(crate) struct Iter<'a> {
    inner: std::collections::hash_map::Iter<'a, String, Pin<Box<UnsafeCell<CacheDbNode>>>>,
    parent: &'a CacheDb,
}
pub(crate) struct IterMut<'a> {
    inner: std::collections::hash_map::IterMut<'a, String, Pin<Box<UnsafeCell<CacheDbNode>>>>,
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
#[allow(private_interfaces)]
pub(crate) enum Entry<'a> {
    Occupied { key: String, value: *mut CacheDbNode },
    Vacant { key: String, parent: &'a mut CacheDb },
}

impl CacheDb {
    pub fn with_capacity(capacity: usize) -> Self {
        CacheDb {
            map: HashMap::with_capacity(capacity + 10),
            head: None,
            tail: None,
            keys_with_expiry: 0,
            capacity,
        }
    }
    /// Extracts (removes) keys and values that fall within the specified token ranges.
    /// This is a mutable operation that modifies the cache by taking out relevant entries.
    ///
    /// # Returns
    /// A HashMap containing the extracted keys and values
    pub(crate) fn take_subset(&mut self, token_ranges: Vec<Range<u64>>) -> CacheDb {
        // If no ranges, return empty HashMap
        if token_ranges.is_empty() {
            return CacheDb::with_capacity(self.capacity);
        }
        let gen_default = || {
            Box::pin(UnsafeCell::new(CacheDbNode {
                key: String::new(),
                value: Box::new(CacheValue { value: String::new(), expiry: None }),
                prev: None,
                next: None,
            }))
        };

        let mut extracted_map = HashMap::new();
        let mut extracted_expiry_count = 0;
        let self_mut = unsafe { std::mem::transmute::<&Self, &mut Self>(self) };
        self.map.retain(|_, v| {
            let node = unsafe { &*v.get() };
            let is_extract_target = token_ranges.iter().any(|range| {
                let hash = fnv_1a_hash(&node.key);
                range.contains(&hash)
            });

            if is_extract_target {
                self_mut.detach(v.get());
                if v.get_mut().value.has_expiry() {
                    extracted_expiry_count += 1;
                }
                let mut value = gen_default();
                std::mem::swap(&mut value, v);
                extracted_map.insert(node.key.clone(), value);
            }

            !is_extract_target
        });
        self.keys_with_expiry -= extracted_expiry_count;
        let head = extracted_map.values().next().map(|v| v.get() as *mut CacheDbNode);
        let tail = extracted_map.values().last().map(|v| v.get() as *mut CacheDbNode);
        // Reconnect nodes
        extracted_map.values_mut().fold(None, |prev, v| {
            let v_ptr = v.get();
            let v_ref = v.get_mut();
            if let Some(prev_node) = prev {
                v_ref.prev = Some(prev_node);
                unsafe { (*prev_node).next = Some(v_ptr) };
            }
            v_ref.next = None;
            Some(v_ptr)
        });
        CacheDb {
            map: extracted_map,
            head,
            tail,
            capacity: self.capacity,
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
        self.map.contains_key(key)
    }
    #[inline]
    pub fn get(&self, key: &str) -> Option<&CacheValue> {
        // SAFETY: mut needed by refreshing LRU order. it won't change inner data
        unsafe { std::mem::transmute::<&Self, &mut Self>(self).get_mut(key) }
            .map(|v| v as &CacheValue)
    }
    #[inline]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut CacheValue> {
        let self_mut = unsafe { std::mem::transmute::<&Self, &mut Self>(self) };
        if let Some(node) = self.map.get_mut(key) {
            self_mut.move_to_head(node.get());
            Some(node.get_mut().value.as_mut())
        } else {
            None
        }
    }
    #[inline]
    pub fn insert(&mut self, key: String, value: CacheValue) {
        let self_mut = unsafe { std::mem::transmute::<&Self, &mut Self>(self) };
        if let Some(existing_node) = self.map.get_mut(&key) {
            existing_node.get_mut().value = Box::new(value);
            self_mut.move_to_head(existing_node.get());
        } else {
            // Remove tail if exceeding capacity
            if self.map.len() >= self.capacity {
                self.remove_tail();
            }
            if value.has_expiry() {
                self.keys_with_expiry += 1;
            }
            let value = Box::pin(UnsafeCell::new(CacheDbNode {
                key: key.clone(),
                value: Box::new(value),
                prev: None,
                next: None,
            }));
            self.push_front(value.get());
            self.map.insert(key, value);
        }
    }
    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<CacheValue> {
        if let Some(mut node) = self.map.remove(key) {
            self.detach(node.get());
            if node.get_mut().value.has_expiry() {
                self.keys_with_expiry -= 1;
            }
            let value = Pin::into_inner(node).into_inner().value;
            Some(*value)
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
        self.map.contains_key(key)
    }

    /* LRU list helpers */
    #[inline]
    fn move_to_head(&mut self, node: *mut CacheDbNode) {
        if self.head == Some(node) {
            // Already at head, no need to move
            return;
        }

        let node = unsafe { &mut *node };
        let prev = node.prev;
        let next = node.next;
        if let Some(prev_node) = prev {
            unsafe { (*prev_node).next = next };
        }
        if let Some(next_node) = next {
            unsafe { (*next_node).prev = prev };
        } else {
            self.tail = node.prev;
        }
        let old_head = self.head.take();
        if let Some(old_head_node) = old_head {
            unsafe { (*old_head_node).prev = Some(node) };
            node.next = Some(old_head_node);
        }
        node.prev = None;
        self.head = Some(node);
    }
    /// Remove element and update links
    fn remove_tail(&mut self) {
        if let Some(tail) = self.tail {
            let tail_node = unsafe { &mut *tail };
            if let Some(prev) = tail_node.prev {
                unsafe { (*prev).next = None };
                self.tail = Some(prev);
            } else {
                self.head = None;
                self.tail = None;
            }
            self.map.remove(&tail_node.key);
            if tail_node.value.has_expiry() {
                self.keys_with_expiry -= 1;
            }
        } else {
            error!("CacheDb: Attempted to remove tail from an empty cache");
        }
    }
    /// detach never remove element from map
    #[inline]
    fn detach(&mut self, node: *mut CacheDbNode) {
        let node = unsafe { &mut *node };
        if let Some(prev) = node.prev {
            unsafe { (*prev).next = node.next };
        } else {
            self.head = node.next;
        }
        if let Some(next) = node.next {
            unsafe { (*next).prev = node.prev };
        } else {
            self.tail = node.prev;
        }
        node.prev = None;
        node.next = None;
    }
    #[inline]
    fn push_front(&mut self, node: *mut CacheDbNode) {
        let node = unsafe { &mut *node };
        node.prev = None;
        node.next = self.head;
        if let Some(old_head) = self.head {
            unsafe { (*old_head).prev = Some(node) };
        } else {
            self.tail = Some(node);
        }
        self.head = Some(node);
    }
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter { inner: self.map.iter(), parent: self }
    }
    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_> {
        let parent = self as *mut CacheDb;
        // SAFETY: to make lifetime work
        let parent = unsafe { &mut *parent };
        let inner = self.map.iter_mut();
        IterMut { inner, parent }
    }
    #[inline]
    pub fn entry(&mut self, key: String) -> Entry<'_> {
        if let Some(node) = self.map.get(&key) {
            let node = node.get();
            self.move_to_head(node);
            Entry::Occupied { key, value: node }
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
        self.head = None;
        self.tail = None;
        self.keys_with_expiry = 0;
    }
    #[cfg(test)]
    fn validate(&self) -> bool {
        if self.map.is_empty() {
            return self.head.is_none() && self.tail.is_none();
        }
        if self.head.is_none() || self.tail.is_none() {
            error!("CacheDb: Inconsistent state, head or tail is None while map is not empty");
            return false;
        }
        // Check head
        let head = self.head.unwrap();
        if unsafe { &*head }.prev.is_some() {
            error!("CacheDb: Head node has a previous pointer");
            return false;
        }
        let mut current = unsafe { &*head }.next;
        let mut before = head;
        let mut count = 1;
        while let Some(node_ptr) = current {
            count += 1;
            let node = unsafe { &*node_ptr };
            if node.prev.is_none() || before != node.prev.unwrap() {
                error!(
                    "CacheDb: Node does not have a valid previous pointer or it does not match the expected previous node"
                );
                return false;
            }
            if node.next.is_some() {
                if let Some(next_ptr) = node.next {
                    before = node_ptr;
                    current = Some(next_ptr);
                } else {
                    error!("CacheDb: Node has next pointer but it is None");
                    return false;
                }
            } else {
                if self.tail.unwrap() != node_ptr {
                    error!("CacheDb: Tail pointer does not match last node");
                    return false;
                }
                break;
            }
        }
        if count != self.map.len() {
            error!("CacheDb: Node count does not match map length");
            return false;
        }
        true
    }
}
impl<'a> Entry<'a> {
    #[inline]
    fn insert_parent(
        parent: &'a mut CacheDb,
        k: String,
        v: CacheValue,
    ) -> &'a mut Pin<Box<UnsafeCell<CacheDbNode>>> {
        let parent_copy = unsafe { std::mem::transmute::<&mut CacheDb, &mut CacheDb>(parent) };
        if let Some(existing_node) = parent.map.get_mut(&k) {
            existing_node.get_mut().value = Box::new(v);
            parent_copy.move_to_head(existing_node.get());
            existing_node
        } else {
            // Remove tail if exceeding capacity
            if parent_copy.map.len() >= parent.capacity {
                parent_copy.remove_tail();
            }
            if v.has_expiry() {
                parent.keys_with_expiry += 1;
            }
            let v = Box::pin(UnsafeCell::new(CacheDbNode {
                key: k.clone(),
                value: Box::new(v),
                prev: None,
                next: None,
            }));
            parent_copy.push_front(v.get());
            parent_copy.map.entry(k).or_insert(v)
        }
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
            | Entry::Occupied { value, .. } => unsafe { &mut *value }.value.as_mut(),
            | Entry::Vacant { key, parent } => {
                Self::insert_parent(parent, key, default).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn or_insert_with<F: FnOnce() -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => unsafe { &mut *value }.value.as_mut(),
            | Entry::Vacant { key, parent } => {
                let val = f();
                Self::insert_parent(parent, key, val).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn or_insert_with_key<F: FnOnce(&String) -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => unsafe { &mut *value }.value.as_mut(),
            | Entry::Vacant { key, parent } => {
                let val = f(&key);
                Self::insert_parent(parent, key, val).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn and_modify<F: FnOnce(&mut CacheValue)>(self, f: F) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, value } => {
                let value = unsafe { &mut *value };
                f(&mut value.value);
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => Entry::Vacant { key, parent },
        }
    }

    #[inline]
    pub fn insert_entry(self, new_val: CacheValue) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, value } => {
                unsafe { &mut *value }.value = Box::new(new_val);
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => {
                let v = Self::insert_parent(parent, key.clone(), new_val);
                Entry::Occupied { key, value: v.get() }
            },
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a String, &'a CacheValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            // SAFETY: Access data of UnsafeCell
            let value = unsafe { &*v.get() };
            (k, value.value.as_ref())
        })
    }
}

impl<'a> Iterator for IterMut<'a> {
    type Item = (&'a String, &'a mut CacheValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k, v.get_mut().value.as_mut()))
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
        let mut cache = CacheDb::with_capacity(100);
        let ranges: Vec<Range<u64>> = Vec::new();

        let result = cache.take_subset(ranges);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_keys_for_ranges_no_matches() {
        let mut cache = CacheDb::with_capacity(100);
        cache.insert("key1".to_string(), CacheValue::new("value".to_string()));

        // Create a range that doesn't include our key's hash
        let key_hash = fnv_1a_hash("key1");
        let ranges = vec![(key_hash + 1000)..u64::MAX];

        let result = cache.take_subset(ranges);
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

        cache.insert(
            "key999".to_string(),
            CacheValue::new("value3".to_string()).with_expiry(expiry),
        );
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

        assert!(cache.get("key1").is_none());

        cache.insert("key1".to_string(), CacheValue::new("value1".to_string()).with_expiry(expiry));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.keys_with_expiry(), 1);
        assert!(cache.get("key1").is_some());

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
        assert_eq!(cache.get("key50").unwrap().value(), "modified_value");

        cache.entry("key50".to_string()).or_insert(CacheValue::new("new_value".to_string()));
        assert_eq!(cache.get("key50").unwrap().value(), "modified_value");

        cache.entry("key999".to_string()).and_modify(|v| {
            v.value = "modified_value".to_string();
        });
        assert!(cache.get("key999").is_none());

        assert!(cache.validate(), "CacheDb validation failed");

        assert_eq!(cache.len(), 100);
        assert_eq!(cache.keys_with_expiry(), 100);
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.keys_with_expiry(), 0);
    }
}
