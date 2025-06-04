/// LRU Cache Implementation using Slab for stable memory layout
///
/// A "slab" is a contiguous block of memory (often consisting of one or more physical pages)
/// that is allocated to a specific cache. This slab is then divided into a number of fixed-size slots,
/// each capable of holding an object of the type managed by that cache.
///
/// ```text
///       +-------------------+
///       | HashMap<K, usize> |
///       | (key -> slab idx) |
///       +---------+---------+
///                 |
///                 v
///      +----------------------+
///      |    Slab<Node<K, V>>  |
///      +----------+-----------+
///                 |
///                 v
///  +------+ <-> +------+ <-> +------+
///  | Node |     | Node |     | Node |    <- Usage order (MRU <-> LRU)
///  +------+     +------+     +------+
/// ```
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::vec;

use crate::domains::caches::cache_objects::THasExpiry;

#[derive(Debug, Clone)]
struct Slab<T> {
    data: Vec<Option<T>>,
    free_list: VecDeque<usize>, // managing available indices
}

impl<T: Clone> Slab<T> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            data: vec![None; cap + 1], // +1 to reserve index 0, ensuring 1-based indexing for nodes
            free_list: (1..=cap).collect(), // Start at 1 to avoid using index 0
        }
    }

    fn insert(&mut self, value: T) -> Option<usize> {
        self.free_list.pop_front().map(|idx| {
            self.data[idx] = Some(value);
            idx
        })
    }

    fn remove(&mut self, index: usize) -> Option<T> {
        if index < self.data.len() && self.data[index].is_some() {
            self.free_list.push_back(index);
            self.data[index].take()
        } else {
            None
        }
    }

    fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)?.as_ref()
    }

    fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index)?.as_mut()
    }
}

#[derive(Debug, Clone)]
struct Node<K: Debug + Clone, V: Debug + Clone> {
    key: K,
    value: V,
    prev: Option<usize>, // pointer
    next: Option<usize>, // pointer
}

pub struct LruCache<K: Eq + std::hash::Hash + Debug + Clone, V: Debug + Clone> {
    map: HashMap<K, usize>, // Key -> slab index
    slab: Slab<Node<K, V>>,
    head: Option<usize>, // most recently used (MRU)
    tail: Option<usize>, // least recently used (LRU)
    capacity: usize,
    current_size: usize,
    pub(crate) keys_with_expiry: usize, // Placeholder for future use
}

impl<K: Eq + Hash + Clone + Debug, V: Debug + Clone + THasExpiry> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        LruCache {
            map: HashMap::with_capacity(capacity),
            slab: Slab::with_capacity(capacity),
            head: None,
            tail: None,
            capacity,
            current_size: 0,
            keys_with_expiry: 0, // Placeholder for future use
        }
    }
    pub fn len(&self) -> usize {
        self.current_size
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.map.keys()
    }

    pub fn iter(&self) -> LruIter<K, V> {
        LruIter { cache: self, current: self.head }
    }

    pub fn entry(&mut self, key: K) -> Entry<K, V> {
        if let Some(&index) = self.map.get(&key) {
            Entry::Occupied(OccupiedEntry { cache: self, index })
        } else {
            Entry::Vacant(VacantEntry { cache: self, key })
        }
    }

    fn move_to_head(&mut self, index: usize) {
        if self.head == Some(index) {
            return; // Already at head
        }

        // Get node to move and collect its prev and next indices
        let (prev_idx, next_idx) = {
            let node = self.slab.get(index).expect("Node not found");
            (node.prev, node.next)
        };

        // Update prev node's next pointer, if prev exists
        if let Some(prev_idx) = prev_idx {
            let prev_node = self.slab.get_mut(prev_idx).expect("Prev node not found");
            prev_node.next = next_idx;
        }

        // Update next node's prev pointer, if next exists
        if let Some(next_idx) = next_idx {
            let next_node = self.slab.get_mut(next_idx).expect("Next node not found");
            next_node.prev = prev_idx;
        }

        // Update tail if the node was the tail
        if self.tail == Some(index) {
            self.tail = prev_idx;
        }

        // Move node to head
        let node = self.slab.get_mut(index).expect("Node not found");
        node.prev = None;
        node.next = self.head;

        if let Some(old_head_idx) = self.head {
            let old_head = self.slab.get_mut(old_head_idx).expect("Old head not found");
            old_head.prev = Some(index);
        } else {
            // List was empty, so this node is also the tail
            self.tail = Some(index);
        }

        self.head = Some(index);
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(&index) = self.map.get(key) {
            self.move_to_head(index);
            Some(&self.slab.get(index).expect("Node not found").value)
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.slab = Slab::with_capacity(self.capacity);
        self.head = None;
        self.tail = None;
        self.current_size = 0;
        self.keys_with_expiry = 0; // Reset expiry count
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(&index) = self.map.get(key) {
            // Remove from map
            self.map.remove(key);

            // Remove from slab
            let node = self.slab.remove(index)?;
            self.current_size -= 1;

            // Update head and tail pointers
            if self.head == Some(index) {
                self.head = node.next; // Move head to next
            }
            if self.tail == Some(index) {
                self.tail = node.prev; // Move tail to prev
            }

            // Update neighbors' pointers
            if let Some(prev_idx) = node.prev {
                if let Some(prev_node) = self.slab.get_mut(prev_idx) {
                    prev_node.next = node.next;
                }
            }
            if let Some(next_idx) = node.next {
                if let Some(next_node) = self.slab.get_mut(next_idx) {
                    next_node.prev = node.prev;
                }
            }

            if node.value.has_expiry() {
                self.keys_with_expiry -= 1; // Decrement if this node had an expiry
            }
            Some(node.value)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if let Some(&index) = self.map.get(&key) {
            // Update existing node
            let node = self.slab.get_mut(index).expect("Node not found");
            node.value = value;
            self.move_to_head(index);
        } else {
            if self.current_size >= self.capacity {
                if let Some(tail_idx) = self.tail {
                    let (tail_key, prev_idx, had_expiry) = {
                        let tail_node = self.slab.get(tail_idx).expect("Tail node not found");
                        (tail_node.key.clone(), tail_node.prev, tail_node.value.has_expiry())
                    };

                    self.map.remove(&tail_key);
                    self.slab.remove(tail_idx);

                    // Decrement keys_with_expiry if the evicted node had an expiry
                    if had_expiry {
                        self.keys_with_expiry -= 1;
                    }

                    self.tail = prev_idx;
                    if let Some(new_tail_idx) = self.tail {
                        let new_tail = self.slab.get_mut(new_tail_idx).expect("New tail not found");
                        new_tail.next = None;
                    } else {
                        self.head = None;
                    }

                    self.current_size -= 1;
                }
            }

            if value.has_expiry() {
                self.keys_with_expiry += 1;
            }
            let new_node = Node { key: key.clone(), value, prev: None, next: None };
            let new_idx = self.slab.insert(new_node).expect("Slab should have space");
            self.map.insert(key, new_idx);
            self.current_size += 1;
            self.move_to_head(new_idx);
        }
    }
}

pub(crate) struct LruIter<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone> {
    pub(crate) cache: &'a LruCache<K, V>,
    pub(crate) current: Option<usize>,
}

impl<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone> Iterator for LruIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.current?;
        let node = self.cache.slab.get(index)?;

        self.current = node.next; // advance for next iteration
        Some((&node.key, &node.value))
    }
}

pub(crate) enum Entry<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone + THasExpiry> Entry<'a, K, V> {
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            | Entry::Occupied(entry) => entry.into_mut(),
            | Entry::Vacant(entry) => entry.insert(default),
        }
    }
}

pub(crate) struct OccupiedEntry<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone> {
    cache: &'a mut LruCache<K, V>,
    index: usize,
}

impl<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone + THasExpiry> OccupiedEntry<'a, K, V> {
    pub fn into_mut(self) -> &'a mut V {
        self.cache.move_to_head(self.index);
        &mut self.cache.slab.get_mut(self.index).expect("Node not found").value
    }
}

pub(crate) struct VacantEntry<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone> {
    cache: &'a mut LruCache<K, V>,
    key: K,
}

impl<'a, K: Eq + Hash + Debug + Clone, V: Debug + Clone + THasExpiry> VacantEntry<'a, K, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        self.cache.put(self.key.clone(), value);
        // Get the index of the newly inserted key (which should be at head)
        let index = *self.cache.map.get(&self.key).expect("Key should exist after put");
        &mut self.cache.slab.get_mut(index).expect("Node not found").value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::caches::cache_objects::CacheValue;
    use chrono::Utc;

    impl THasExpiry for &'static str {
        fn has_expiry(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_lru_basic() {
        let mut cache = LruCache::new(2);

        cache.put(1, "one");
        cache.put(2, "two");

        assert_eq!(cache.get(&1), Some(&"one")); // 1 is now MRU
        assert_eq!(cache.get(&2), Some(&"two")); // 2 is now MRU (list: 2 -> 1)

        cache.put(3, "three"); // Evicts 1 (LRU)
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&"two")); // 2 is now LRU
        assert_eq!(cache.get(&3), Some(&"three")); // 3 is MRU (list: 3 -> 2)

        cache.put(4, "four"); // Evicts 2 (LRU)
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some(&"three")); // 3 is LRU
        assert_eq!(cache.get(&4), Some(&"four")); // 4 is MRU (list: 4 -> 3)
    }

    #[test]
    fn test_lru_update_existing() {
        let mut cache = LruCache::new(2);
        cache.put(1, "one");
        cache.put(2, "two");

        assert_eq!(cache.get(&1), Some(&"one")); // 1 is now MRU

        cache.put(2, "updated_two"); // Update value, 2 becomes MRU. List: 2 -> 1
        assert_eq!(cache.get(&2), Some(&"updated_two")); // 2 is now MRU
        assert_eq!(cache.get(&1), Some(&"one")); // 1 is now MRU

        cache.put(3, "three"); // Evicts 2. List: 3 -> 1
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&1), Some(&"one")); // 1 is now MRU
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_lru_capacity_one() {
        let mut cache = LruCache::new(1);
        cache.put(1, "one");
        assert_eq!(cache.get(&1), Some(&"one"));

        cache.put(2, "two"); // Evicts 1
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&"two"));

        cache.put(2, "updated_two"); // Update existing
        assert_eq!(cache.get(&2), Some(&"updated_two")); // Still "two"
    }

    #[test]
    fn test_lru_get_non_existent() {
        let mut cache = LruCache::new(2);
        cache.put(1, "A");
        assert_eq!(cache.get(&2), None); // No 2 yet
        cache.put(2, "B");
        assert_eq!(cache.get(&3), None); // No 3 yet
    }

    #[test]
    fn test_lru_remove_key() {
        let mut cache = LruCache::new(3);
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        // Remove middle key
        let removed = cache.remove(&2);
        assert_eq!(removed, Some("two"));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.len(), 2);

        // Remaining keys should still be accessible
        assert_eq!(cache.get(&1), Some(&"one"));
        assert_eq!(cache.get(&3), Some(&"three"));
    }

    #[test]
    fn test_lru_remove_head_tail_update() {
        let mut cache = LruCache::new(2);
        cache.put(1, "one");
        cache.put(2, "two"); // MRU

        // Remove MRU
        assert_eq!(cache.head, Some(*cache.map.get(&2).unwrap()));
        cache.remove(&2);
        assert_eq!(cache.head, Some(*cache.map.get(&1).unwrap()));

        // Remove last remaining
        cache.remove(&1);
        assert_eq!(cache.head, None);
        assert_eq!(cache.tail, None);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_iter_empty_cache() {
        let cache = LruCache::<i32, CacheValue>::new(3);
        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_iter_single_item() {
        let mut cache = LruCache::new(3);
        cache.put(1, CacheValue::new("one".into()));

        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], (&1, &CacheValue::new("one".into())));
    }

    #[test]
    fn test_iter_multiple_items_mru_order() {
        let mut cache = LruCache::new(3);
        cache.put(1, CacheValue::new("one".into()));
        cache.put(2, CacheValue::new("two".into()));
        cache.put(3, CacheValue::new("three".into()));

        // Should iterate from MRU (3) to LRU (1)
        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], (&3, &CacheValue::new("three".into()))); // MRU
        assert_eq!(items[1], (&2, &CacheValue::new("two".into())));
        assert_eq!(items[2], (&1, &CacheValue::new("one".into()))); // LRU
    }

    #[test]
    fn test_iter_after_access_reorders() {
        let mut cache = LruCache::new(3);
        cache.put(1, CacheValue::new("one".into()));
        cache.put(2, CacheValue::new("two".into()));
        cache.put(3, CacheValue::new("three".into()));

        // Access key 1, making it MRU
        let _ = cache.get(&1);

        // Should iterate from MRU (1) to LRU (2)
        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], (&1, &CacheValue::new("one".into()))); // Now MRU
        assert_eq!(items[1], (&3, &CacheValue::new("three".into())));
        assert_eq!(items[2], (&2, &CacheValue::new("two".into()))); // Now LRU
    }

    #[test]
    fn test_iter_doesnt_modify_cache() {
        let mut cache = LruCache::new(3);
        cache.put(1, CacheValue::new("one".into()));
        cache.put(2, CacheValue::new("two".into()));

        // Store original state
        let original_len = cache.len();
        let original_head = cache.head;
        let original_tail = cache.tail;

        // Iterate over cache
        let _items: Vec<_> = cache.iter().collect();

        // Verify cache state unchanged
        assert_eq!(cache.len(), original_len);
        assert_eq!(cache.head, original_head);
        assert_eq!(cache.tail, original_tail);
    }

    #[test]
    fn test_entry_vacant_or_insert() {
        let mut cache = LruCache::new(3);

        // Insert new key via entry
        let value = cache.entry(1).or_insert("one");
        assert_eq!(value, &mut "one");
        assert_eq!(cache.len(), 1);

        // Verify it was actually inserted
        assert_eq!(cache.get(&1), Some(&"one"));
    }

    #[test]
    fn test_entry_occupied_or_insert() {
        let mut cache = LruCache::new(3);
        cache.put(1, "original");

        // Try to insert via entry - should return existing value
        let value = cache.entry(1).or_insert("new");
        assert_eq!(value, &mut "original");
        assert_eq!(cache.len(), 1);

        // Verify original value is unchanged
        assert_eq!(cache.get(&1), Some(&"original"));
    }

    #[test]
    fn test_entry_occupied_moves_to_head() {
        let mut cache = LruCache::new(3);
        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        // Access key 1 via entry - should move it to head
        let _value = cache.entry(1).or_insert("unused");

        // Verify order changed (1 should now be MRU)
        let items: Vec<_> = cache.iter().collect();
        assert_eq!(items[0], (&1, &"one"));
    }

    #[test]
    fn test_entry_mutable_reference_modification() {
        let mut cache = LruCache::new(3);

        // Insert and modify via entry
        {
            let value = cache.entry(1).or_insert(CacheValue::new("original".into()));
            value.value = "modified".to_string();
        }

        // Verify modification persisted
        assert_eq!(cache.get(&1), Some(&CacheValue::new("modified".into())));
    }

    #[test]
    fn test_entry_multiple_operations() {
        let mut cache = LruCache::new(2);

        // Insert first key
        cache.entry(1).or_insert(CacheValue::new("one".into()));

        // Insert second key
        cache.entry(2).or_insert(CacheValue::new("two".into()));

        // Modify first key via entry (should move to head)
        {
            let value = cache.entry(1).or_insert(CacheValue::new("unused".into()));
            value.value = "one_modified".to_string();
        }

        // Insert third key (should evict key 2, since 1 is now MRU)
        cache.entry(3).or_insert(CacheValue::new("three".into()));

        // Verify state
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&1), Some(&CacheValue::new("one_modified".into())));
        assert_eq!(cache.get(&2), None); // Should be evicted
        assert_eq!(cache.get(&3), Some(&CacheValue::new("three".into())));
    }

    #[test]
    fn test_lru_with_expiry() {
        let mut cache = LruCache::new(3);
        let expiry = Some(Utc::now() + chrono::Duration::minutes(10));
        assert_eq!(cache.capacity, 3);

        for i in 0..3 {
            cache.put(
                format!("key{}", i),
                CacheValue::new(format!("value{}", i)).with_expiry(expiry),
            );
        }
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.keys_with_expiry, 3);

        cache.put("key999".to_string(), CacheValue::new("value3".to_string()).with_expiry(expiry));
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.keys_with_expiry, 3);

        cache
            .entry("key998".to_string())
            .or_insert(CacheValue::new("value4".to_string()).with_expiry(expiry));
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.keys_with_expiry, 3);

        assert!(cache.get(&"key2".to_string()).is_some());
    }
}
