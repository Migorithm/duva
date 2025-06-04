///       +-------------------+
///       | HashMap<K, usize>  |
///       |(key -> slab idx)   |
///       +----------+--------+
///                  |
///                  v
///       +------------------------+
///       |      Slab<Node<K, V>>  |
///       +------------------------+
///                  |
///                  v
///  +------+ <-> +------+ <-> +------+
///  | Node |     | Node |     | Node |     <- Usage order (MRU <-> LRU)
///  +------+     +------+     +------+
///
/// Memory locality: Nodes are in slab, likely packed close together.
/// No heap allocation on put/get, just reuse slab slots.
/// Stable indices: You don’t deal with pointer safety or pinning.
/// No fragmentation: The slab reuses freed slots.
/// Predictable memory usage: Great for embedded or low-latency apps.
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::vec;

use crate::domains::caches::cache_objects::CacheValue;

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
struct Node<K: Debug> {
    key: K,
    value: CacheValue,
    prev: Option<usize>, // pointer
    next: Option<usize>, // pointer
}

struct LruCache<K: Eq + std::hash::Hash + Debug> {
    map: HashMap<K, usize>, // Key -> slab index
    slab: Slab<Node<K>>,    // stable storage
    head: Option<usize>,    // most recently used (MRU)
    tail: Option<usize>,    // least recently used (LRU)
    capacity: usize,
    current_size: usize,
}

impl<K: Eq + Hash + Clone + Debug> LruCache<K> {
    pub fn new(capacity: usize) -> Self {
        LruCache {
            map: HashMap::with_capacity(capacity),
            slab: Slab::with_capacity(capacity),
            head: None,
            tail: None,
            capacity,
            current_size: 0,
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

    pub fn get(&mut self, key: &K) -> Option<&CacheValue> {
        if let Some(&index) = self.map.get(key) {
            self.move_to_head(index);
            Some(&self.slab.get(index).expect("Node not found").value)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: CacheValue) {
        if let Some(&index) = self.map.get(&key) {
            // Update existing node
            let node = self.slab.get_mut(index).expect("Node not found");
            node.value = value;
            self.move_to_head(index);
        } else {
            // Evict if necessary
            if self.current_size >= self.capacity {
                if let Some(tail_idx) = self.tail {
                    // Get the key and prev pointer before removing the node
                    let (tail_key, prev_idx) = {
                        let tail_node = self.slab.get(tail_idx).expect("Tail node not found");
                        (tail_node.key.clone(), tail_node.prev)
                    };

                    self.map.remove(&tail_key);
                    self.slab.remove(tail_idx);

                    // Update tail
                    self.tail = prev_idx;
                    if let Some(new_tail_idx) = self.tail {
                        let new_tail = self.slab.get_mut(new_tail_idx).expect("New tail not found");
                        new_tail.next = None;
                    } else {
                        // List is now empty
                        self.head = None;
                    }

                    self.current_size -= 1;
                }
            }

            // Insert new node
            let new_node = Node { key: key.clone(), value, prev: None, next: None };

            let new_idx = self.slab.insert(new_node).expect("Slab should have space");
            self.map.insert(key, new_idx);
            self.current_size += 1;
            self.move_to_head(new_idx);
        }
    }

    #[cfg(test)]
    fn debug_print_list(&self) {
        println!("--- LRU List (MRU to LRU) ---");
        let mut current = self.head;
        let mut count = 0;
        while let Some(idx) = current {
            if let Some(node) = self.slab.get(idx) {
                print!("({}, {}) -> ", format!("{:?}", node.key), format!("{:?}", node.value));
                current = node.next;
                count += 1;
            } else {
                println!("Error: Node not found at index {}", idx);
                break;
            }
            if count > self.capacity * 2 {
                println!("... (list too long, potential cycle)");
                break;
            }
        }
        println!("None");
        println!("Head: {:?}, Tail: {:?}, Size: {}", self.head, self.tail, self.current_size);
        println!("------------------------------");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic() {
        let mut cache = LruCache::new(2);

        cache.put(1, CacheValue::new("one".to_string()));
        cache.put(2, CacheValue::new("two".to_string()));

        assert_eq!(cache.get(&1), Some(&CacheValue::new("one".into()))); // 1 is now MRU
        assert_eq!(cache.get(&2), Some(&CacheValue::new("two".into()))); // 2 is now MRU (list: 2 -> 1)

        cache.put(3, CacheValue::new("three".into())); // Evicts 1 (LRU)
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&CacheValue::new("two".into()))); // 2 is now LRU
        assert_eq!(cache.get(&3), Some(&CacheValue::new("three".into()))); // 3 is MRU (list: 3 -> 2)

        cache.put(4, CacheValue::new("four".into())); // Evicts 2 (LRU)
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some(&CacheValue::new("three".into()))); // 3 is LRU
        assert_eq!(cache.get(&4), Some(&CacheValue::new("four".into()))); // 4 is MRU (list: 4 -> 3)
    }

    #[test]
    fn test_lru_update_existing() {
        let mut cache = LruCache::new(2);
        cache.put(1, CacheValue::new("one".to_string()));
        cache.put(2, CacheValue::new("two".to_string()));

        assert_eq!(cache.get(&1), Some(&CacheValue::new("one".into()))); // 1 is now MRU

        cache.put(2, CacheValue::new("updated_two".into())); // Update value, 2 becomes MRU. List: 2 -> 1
        assert_eq!(cache.get(&2), Some(&CacheValue::new("updated_two".into()))); // 2 is now MRU
        assert_eq!(cache.get(&1), Some(&CacheValue::new("one".into()))); // 1 is now MRU

        cache.put(3, CacheValue::new("three".into())); // Evicts 2. List: 3 -> 1
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&1), Some(&CacheValue::new("one".into()))); // 1 is now MRU
        assert_eq!(cache.get(&3), Some(&CacheValue::new("three".into())));
    }

    #[test]
    fn test_lru_capacity_one() {
        let mut cache = LruCache::new(1);
        cache.put(1, CacheValue::new("one".to_string()));
        assert_eq!(cache.get(&1), Some(&CacheValue::new("one".into())));

        cache.put(2, CacheValue::new("two".into())); // Evicts 1
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some(&CacheValue::new("two".into())));

        cache.put(2, CacheValue::new("updated_two".into())); // Update existing
        assert_eq!(cache.get(&2), Some(&CacheValue::new("updated_two".into()))); // Still "two"
    }

    #[test]
    fn test_lru_get_non_existent() {
        let mut cache = LruCache::new(2);
        cache.put(1, CacheValue::new("A".into()));
        assert_eq!(cache.get(&2), None); // No 2 yet
        cache.put(2, CacheValue::new("B".into()));
        assert_eq!(cache.get(&3), None); // No 3 yet
    }
}
