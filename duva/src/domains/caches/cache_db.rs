#![allow(dead_code)]
#![allow(mutable_transmutes)]
mod iter;
use iter::*;
mod cache_node;
use cache_node::*;
mod entry;
use entry::*;
#[cfg(test)]
mod tests;
use crate::domains::caches::cache_objects::CacheValue;
use crate::domains::cluster_actors::hash_ring::fnv_1a_hash;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;

pub(crate) struct CacheDb {
    map: HashMap<String, Pin<Box<UnsafeCell<CacheNode>>>>,
    head: Option<NodeRawPtr>,
    tail: Option<NodeRawPtr>,
    capacity: usize,
    // OPTIMIZATION: Add a counter to keep track of the number of keys with expiry
    keys_with_expiry: usize,
}

unsafe impl Send for CacheDb {}
unsafe impl Sync for CacheDb {}

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
                self.detach(node_ptr.into());
                if node_ptr.as_ref().value.has_expiry() {
                    extracted_expiry_count += 1;
                }
                extracted_map.insert(key, node_pin_box);
            }
        }

        self.keys_with_expiry -= extracted_expiry_count;

        // Reconstructing head/tail for the extracted map.
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
            self.push_front(new_node.get().into());
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

        if head.get_node_prev_link().is_some() {
            error!("CacheDb: Head node has a previous pointer");
            return false;
        }
        let mut current = head.get_node_next_link();
        let mut before = head;
        let mut count = 1;
        while let Some(node_ptr) = current {
            count += 1;

            if node_ptr.get_node_prev_link().is_none()
                || before != node_ptr.get_node_prev_link().unwrap()
            {
                error!(
                    "CacheDb: Node does not have a valid previous pointer or it does not match the expected previous node"
                );
                return false;
            }
            if node_ptr.get_node_next_link().is_some() {
                let next_ptr = node_ptr.get_node_next_link().unwrap();
                before = node_ptr;
                current = Some(next_ptr);
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
