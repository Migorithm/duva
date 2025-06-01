#![allow(dead_code)]
#![allow(mutable_transmutes)]

use crate::{
    domains::{caches::cache_objects::CacheValue, cluster_actors::hash_ring::fnv_1a_hash},
    from_to, make_smart_pointer,
};
use std::{cell::UnsafeCell, collections::HashMap, ops::Range, pin::Pin};

pub(crate) struct CacheDb {
    map: HashMap<String, Pin<Box<UnsafeCell<CacheNode>>>>,
    head: Option<NodeRawPtr>,
    tail: Option<NodeRawPtr>,
    capacity: usize,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    keys_with_expiry: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeRawPtr(*mut CacheNode);
from_to!(*mut CacheNode, NodeRawPtr);
make_smart_pointer!(NodeRawPtr, *mut CacheNode);

impl NodeRawPtr {
    #[inline]
    fn set_node_prev_link(self, new_prev: Option<NodeRawPtr>) {
        unsafe {
            (*self.0).prev = new_prev;
        }
    }
    #[inline]
    fn set_node_next_link(self, new_next: Option<NodeRawPtr>) {
        unsafe {
            (*self.0).next = new_next;
        }
    }

    #[inline]
    fn get_node_prev_link(self) -> Option<NodeRawPtr> {
        unsafe { (*(self.0)).prev.clone() }
    }

    #[inline]
    fn get_node_next_link(self) -> Option<NodeRawPtr> {
        unsafe { (*self.0).next.clone() }
    }
    #[inline]
    fn as_ref(&self) -> &CacheNode {
        unsafe { &*self.0 }
    }

    // ! SAFETY: The caller guarantees `ptr` is valid and non-aliased for a mutable reference.
    // ! The `'static` lifetime is a lie, but necessary for the compiler to accept the returned
    // ! reference. The true lifetime must be managed by the caller.(This pattern is common
    // ! when encapsulating raw pointer usage.)
    #[inline]
    fn value_mut(&mut self) -> &'static mut CacheValue {
        unsafe { (&mut *self.0).value.as_mut() }
    }
}

#[derive(Clone)]
struct CacheNode {
    key: String,
    value: Box<CacheValue>,
    prev: Option<NodeRawPtr>,
    next: Option<NodeRawPtr>,
}

unsafe impl Send for CacheDb {}
unsafe impl Sync for CacheDb {}
pub(crate) struct Iter<'a> {
    inner: std::collections::hash_map::Iter<'a, String, Pin<Box<UnsafeCell<CacheNode>>>>,
    parent: &'a CacheDb,
}
pub(crate) struct IterMut<'a> {
    inner: std::collections::hash_map::IterMut<'a, String, Pin<Box<UnsafeCell<CacheNode>>>>,
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
    Occupied { key: String, value: NodeRawPtr },
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
        if token_ranges.is_empty() {
            return CacheDb::with_capacity(self.capacity);
        }

        let mut extracted_map = HashMap::new();
        let mut extracted_expiry_count = 0;

        // Use `drain_filter` if it were stable, or iterate and remove.
        // `retain` is good, but we need to move the ownership of Pin<Box<UnsafeCell<CacheDbNode>>> out.
        // This requires an UNSAFE block for accessing inner data or temporary transmute if we keep `retain`.
        // A safer way is to iterate, collect, and then remove.
        let mut keys_to_extract = Vec::new();
        for (key, node_pin_box) in self.map.iter() {
            let node_ptr: NodeRawPtr = node_pin_box.get().into();
            let is_extract_target = token_ranges.iter().any(|range| {
                let hash = fnv_1a_hash(&node_ptr.as_ref().key);
                range.contains(&hash)
            });

            if is_extract_target {
                keys_to_extract.push(key.clone());
            }
        }

        for key in keys_to_extract {
            if let Some(node_pin_box) = self.map.remove(&key) {
                let node_ptr: NodeRawPtr = node_pin_box.get().into();
                self.detach(node_ptr.into()); // This modifies the linked list's pointers, still unsafe internally.
                if node_ptr.as_ref().value.has_expiry() {
                    extracted_expiry_count += 1;
                }
                extracted_map.insert(key, node_pin_box);
            }
        }

        self.keys_with_expiry -= extracted_expiry_count;

        // Reconstructing head/tail for the extracted map.
        // This part also involves unsafe raw pointer manipulation to build the new linked list.
        let mut current_extracted_head: Option<NodeRawPtr> = None;
        let mut current_extracted_tail: Option<NodeRawPtr> = None;
        let mut prev_node_ptr: Option<NodeRawPtr> = None;

        for node_pin_box in extracted_map.values_mut() {
            let node_ptr: NodeRawPtr = node_pin_box.get().into();
            if current_extracted_head.is_none() {
                current_extracted_head = Some(node_ptr.into());
                node_ptr.set_node_prev_link(None);
            } else {
                // SAFETY: `prev_node_ptr` is valid and `node_ptr` is valid.
                if let Some(p_ptr) = prev_node_ptr.clone() {
                    p_ptr.set_node_next_link(node_ptr.into());
                }
                node_ptr.set_node_prev_link(prev_node_ptr);
            }
            // SAFETY: `node_ptr` is valid.
            node_ptr.set_node_next_link(None);
            current_extracted_tail = Some(node_ptr.into());
            prev_node_ptr = Some(node_ptr.into());
        }

        CacheDb {
            map: extracted_map,
            head: current_extracted_head,
            tail: current_extracted_tail,
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
    pub fn get(&mut self, key: &str) -> Option<&CacheValue> {
        self.get_mut(key).map(|v| v as &CacheValue)
    }

    #[inline]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut CacheValue> {
        if let Some(node) = self.map.get_mut(key) {
            let mut node_ptr: NodeRawPtr = node.get().into();
            self.move_to_head(node_ptr);
            Some(node_ptr.value_mut())
        } else {
            None
        }
    }

    #[inline]
    pub fn insert(&mut self, key: String, value: CacheValue) {
        if let Some(existing_node) = self.map.get_mut(&key) {
            let mut node_ptr: NodeRawPtr = existing_node.get().into();

            *node_ptr.value_mut() = value;
            self.move_to_head(node_ptr);
        } else {
            if self.map.len() >= self.capacity {
                self.remove_tail();
            }
            if value.has_expiry() {
                self.keys_with_expiry += 1;
            }
            let new_node = Box::pin(UnsafeCell::new(CacheNode {
                key: key.clone(),
                value: Box::new(value),
                prev: None,
                next: None,
            }));
            self.push_front(new_node.get().into()); // This has internal unsafe
            self.map.insert(key, new_node);
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<CacheValue> {
        if let Some(mut node) = self.map.remove(key) {
            self.detach(node.get().into());
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

    // Detach given node from the linked list and move it to the head
    #[inline]
    fn move_to_head(&mut self, node: NodeRawPtr) {
        if self.head == Some(node) {
            // Already at head, no need to move
            return;
        }
        self.detach(node);

        if let Some(old_head_node) = self.head.take() {
            old_head_node.set_node_prev_link(Some(node));
            node.set_node_next_link(Some(old_head_node));
        }
        self.head = Some(node);
    }

    /// Remove element and update links
    #[inline]
    fn remove_tail(&mut self) {
        if let Some(tail) = self.tail {
            if let Some(prev) = tail.get_node_prev_link() {
                prev.set_node_next_link(None);
                self.tail = Some(prev);
            } else {
                self.head = None;
                self.tail = None;
            }
            if tail.as_ref().value.has_expiry() {
                self.keys_with_expiry -= 1;
            }
            self.map.remove(&tail.as_ref().key);
        }
    }
    /// detach never remove element from map
    #[inline]
    fn detach(&mut self, node_ptr: NodeRawPtr) {
        let prev_ptr = node_ptr.get_node_prev_link();
        let next_ptr = node_ptr.get_node_next_link();

        if let Some(p_ptr) = prev_ptr {
            p_ptr.set_node_next_link(next_ptr);
        } else {
            self.head = next_ptr;
        }
        if let Some(n_ptr) = next_ptr {
            n_ptr.set_node_prev_link(prev_ptr);
        } else {
            self.tail = prev_ptr;
        }
        node_ptr.set_node_prev_link(None);
        node_ptr.set_node_next_link(None);
    }

    #[inline]
    fn push_front(&mut self, node_ptr: NodeRawPtr) {
        let prev_head = self.head.take();

        node_ptr.set_node_prev_link(None);
        node_ptr.set_node_next_link(prev_head);

        if let Some(oh_ptr) = prev_head {
            oh_ptr.set_node_prev_link(Some(node_ptr));
        } else {
            // If there was no old head, this node is also the tail
            self.tail = Some(node_ptr);
        }
        self.head = Some(node_ptr);
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
            self.move_to_head(node.into());
            Entry::Occupied { key, value: node.into() }
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
        use tracing::error;

        if self.map.is_empty() {
            return self.head.is_none() && self.tail.is_none();
        }
        if self.head.is_none() || self.tail.is_none() {
            error!("CacheDb: Inconsistent state, head or tail is None while map is not empty");
            return false;
        }
        // Check head
        let head = self.head.unwrap();
        if unsafe { &*head.0 }.prev.is_some() {
            error!("CacheDb: Head node has a previous pointer");
            return false;
        }
        let mut current = unsafe { &*head.0 }.next;
        let mut before = head;
        let mut count = 1;
        while let Some(node_ptr) = current {
            count += 1;
            let node = unsafe { &*node_ptr.0 };
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
    ) -> &'a mut Pin<Box<UnsafeCell<CacheNode>>> {
        let parent_copy = unsafe { std::mem::transmute::<&mut CacheDb, &mut CacheDb>(parent) };
        if let Some(existing_node) = parent.map.get_mut(&k) {
            existing_node.get_mut().value = Box::new(v);
            parent_copy.move_to_head(existing_node.get().into());
            existing_node
        } else {
            // Remove tail if exceeding capacity
            if parent_copy.map.len() >= parent.capacity {
                parent_copy.remove_tail();
            }
            if v.has_expiry() {
                parent.keys_with_expiry += 1;
            }
            let v = Box::pin(UnsafeCell::new(CacheNode {
                key: k.clone(),
                value: Box::new(v),
                prev: None,
                next: None,
            }));
            parent_copy.push_front(v.get().into());
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
            | Entry::Occupied { value, .. } => unsafe { &mut *value.0 }.value.as_mut(),
            | Entry::Vacant { key, parent } => {
                Self::insert_parent(parent, key, default).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn or_insert_with<F: FnOnce() -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => unsafe { &mut *value.0 }.value.as_mut(),
            | Entry::Vacant { key, parent } => {
                let val = f();
                Self::insert_parent(parent, key, val).get_mut().value.as_mut()
            },
        }
    }

    #[inline]
    pub fn or_insert_with_key<F: FnOnce(&String) -> CacheValue>(self, f: F) -> &'a mut CacheValue {
        match self {
            | Entry::Occupied { value, .. } => unsafe { &mut *value.0 }.value.as_mut(),
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
                f(&mut unsafe { &mut *value.0 }.value);
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => Entry::Vacant { key, parent },
        }
    }

    #[inline]
    pub fn insert_entry(self, new_val: CacheValue) -> Entry<'a> {
        match self {
            | Entry::Occupied { key, value } => {
                unsafe { &mut *value.0 }.value = Box::new(new_val);
                Entry::Occupied { key, value }
            },
            | Entry::Vacant { key, parent } => {
                let v = Self::insert_parent(parent, key.clone(), new_val);
                Entry::Occupied { key, value: v.get().into() }
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
