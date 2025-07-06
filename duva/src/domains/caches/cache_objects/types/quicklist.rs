use bytes::Bytes;
use std::collections::VecDeque;
// For a true Redis implementation, a crate like 'lzf' would be used.
// use lzf;

#[derive(Debug, Clone, Default)]
struct Ziplist {
    data: Vec<u8>,
}

impl Ziplist {
    /// Appends an entry to the end of the ziplist.
    fn push_back(&mut self, value: &Bytes) {
        self.data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.data.extend_from_slice(value);
    }

    /// Efficiently prepends an entry by rebuilding the data vector once.
    fn lpush(&mut self, value: &Bytes) {
        let mut new_data = Vec::with_capacity(value.len() + 4 + self.data.len());
        new_data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        new_data.extend_from_slice(value);
        new_data.extend_from_slice(&self.data);
        self.data = new_data;
    }

    /// Removes and returns the last entry.
    fn rpop(&mut self) -> Option<Bytes> {
        if self.data.is_empty() {
            return None;
        }

        // This is complex in a real ziplist. We simulate by finding the last entry.
        let mut cursor = 0;
        let mut last_entry_start = 0;
        let mut last_entry_len = 0;

        while cursor < self.data.len() {
            last_entry_start = cursor;
            let len = u32::from_le_bytes(self.data[cursor..cursor + 4].try_into().ok()?) as usize;
            last_entry_len = len;
            cursor += 4 + len;
        }

        let entry_data = Bytes::copy_from_slice(
            &self.data[last_entry_start + 4..last_entry_start + 4 + last_entry_len],
        );
        self.data.truncate(last_entry_start);
        Some(entry_data)
    }

    /// Removes and returns the first entry.
    fn lpop(&mut self) -> Option<Bytes> {
        if self.data.is_empty() {
            return None;
        }
        let len = u32::from_le_bytes(self.data[0..4].try_into().ok()?) as usize;
        let entry_data = Bytes::copy_from_slice(&self.data[4..4 + len]);
        self.data = self.data.split_off(4 + len);
        Some(entry_data)
    }

    /// Decodes the ziplist back into a vector of Bytes.
    fn to_vec(&self) -> Vec<Bytes> {
        let mut entries = Vec::new();
        let mut cursor = 0;
        while cursor < self.data.len() {
            if let Ok(len_bytes) = self.data[cursor..cursor + 4].try_into() {
                let len = u32::from_le_bytes(len_bytes) as usize;
                cursor += 4;
                if cursor + len <= self.data.len() {
                    entries.push(Bytes::copy_from_slice(&self.data[cursor..cursor + len]));
                    cursor += len;
                } else {
                    break; // Corrupted data
                }
            } else {
                break; // Not enough data for length
            }
        }
        entries
    }

    /// Returns the total memory size of the ziplist.
    fn byte_size(&self) -> usize {
        self.data.len()
    }
}

/// Represents a single node in the QuickList, which contains a Ziplist.
#[derive(Debug)]
struct QuickListNode {
    /// The actual data, either uncompressed (Ziplist) or compressed.
    data: Ziplist,
    /// Number of entries in the ziplist.
    entry_count: usize,
    /// Max size setting. Positive for entry count, negative for bytes.
    fill_factor: isize,
}

impl QuickListNode {
    fn new(fill_factor: isize) -> Self {
        Self { data: Ziplist::default(), entry_count: 0, fill_factor }
    }

    /// Checks if a new value can be added without exceeding the fill factor.
    fn is_full(&self, new_value_size: usize) -> bool {
        if self.fill_factor > 0 {
            // Check against entry count
            self.entry_count >= self.fill_factor as usize
        } else {
            // Check against byte size. Redis uses specific negative values like -1 for 4KB, -2 for 8KB.
            let max_bytes = (-self.fill_factor) as usize * 1024;
            self.data.byte_size() + new_value_size + 4 > max_bytes
        }
    }

    fn lpush(&mut self, value: Bytes) {
        self.data.lpush(&value);
        self.entry_count += 1;
    }

    fn rpush(&mut self, value: Bytes) {
        self.data.push_back(&value);
        self.entry_count += 1;
    }

    /// Pops a value from the front or back.
    fn pop(&mut self, from_front: bool) -> Option<Bytes> {
        let result = if from_front { self.data.lpop() } else { self.data.rpop() };

        if result.is_some() {
            self.entry_count -= 1;
        }
        result
    }

    // This split function is no longer central to the push logic,
    // but is kept for potential future use (e.g., in ltrim or other commands).
    fn split(&mut self) -> Self {
        let entries = self.data.to_vec();
        let split_at = entries.len() / 2;

        let (part1, part2) = entries.split_at(split_at);

        let mut new_node = Self::new(self.fill_factor);
        for entry in part2 {
            // Use clone() here. It's cheap and solves the lifetime issue.
            new_node.rpush(entry.clone());
        }

        // Rebuild the current node with the remaining part
        self.data = Ziplist::default();
        self.entry_count = 0;
        for entry in part1 {
            // Use clone() here as well.
            self.rpush(entry.clone());
        }

        new_node
    }
}
/// A memory-optimized list structure, similar to Redis's Quicklist.
#[derive(Debug)]
pub struct QuickList {
    nodes: VecDeque<QuickListNode>,
    len: usize,
    /// Controls node size. > 0 for count, < 0 for KB size (e.g., -2 for 8KB).
    fill_factor: isize,
    /// Head/tail nodes to keep uncompressed. 0 to disable compression.
    compress_depth: usize,
}

impl QuickList {
    /// Creates a new QuickList.
    ///
    /// # Arguments
    /// * `fill_factor`: Max size of a node. If positive, it's the max entry count.
    ///   If negative, it's the max size in KB (e.g., -2 for 8KB).
    /// * `compress_depth`: Number of nodes at the head and tail to *exclude* from compression.
    pub fn new(fill_factor: isize, compress_depth: usize) -> Self {
        Self { nodes: VecDeque::new(), len: 0, fill_factor, compress_depth }
    }

    /// Pushes a value to the front (left) of the list using a fast, iterative approach.
    pub fn lpush(&mut self, value: Bytes) {
        let val_size = value.len();

        // Case 1: The head node exists and is not full.
        if let Some(head) = self.nodes.front_mut() {
            if !head.is_full(val_size) {
                head.lpush(value);
                self.len += 1;
                return;
            }
        }

        // Case 2: The list is empty OR the head node is full.
        // In both scenarios, create a new node at the front for the new value.
        let mut new_node = QuickListNode::new(self.fill_factor);
        new_node.lpush(value);
        self.nodes.push_front(new_node);
        self.len += 1;
    }

    /// Pushes a value to the back (right) of the list using a fast, iterative approach.
    pub fn rpush(&mut self, value: Bytes) {
        let val_size = value.len();

        // Case 1: The tail node exists and is not full.
        if let Some(tail) = self.nodes.back_mut() {
            if !tail.is_full(val_size) {
                tail.rpush(value);
                self.len += 1;
                return;
            }
        }

        // Case 2: The list is empty OR the tail node is full.
        // In both scenarios, create a new node at the back for the new value.
        let mut new_node = QuickListNode::new(self.fill_factor);
        new_node.rpush(value);
        self.nodes.push_back(new_node);
        self.len += 1;
    }

    /// Pops a value from the front (left) of the list.
    pub fn lpop(&mut self) -> Option<Bytes> {
        let val = self.nodes.front_mut()?.pop(true);
        if val.is_some() {
            self.len -= 1;
            if self.nodes.front().unwrap().entry_count == 0 {
                self.nodes.pop_front();
            }
        }
        val
    }

    /// Pops a value from the back (right) of the list.
    pub fn rpop(&mut self) -> Option<Bytes> {
        let val = self.nodes.back_mut()?.pop(false);
        if val.is_some() {
            self.len -= 1;
            if self.nodes.back().unwrap().entry_count == 0 {
                self.nodes.pop_back();
            }
        }
        val
    }

    /// Returns the total number of elements in the list.
    pub fn llen(&self) -> usize {
        self.len
    }

    /// Returns a range of elements. Handles negative indices like Redis.
    pub fn lrange(&self, start: isize, stop: isize) -> Vec<Bytes> {
        if self.len == 0 {
            return vec![];
        }

        let len = self.len as isize;
        let start = if start < 0 { (len + start).max(0) } else { start } as usize;
        let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;

        if start > stop {
            return vec![];
        }

        let mut result = Vec::new();
        let mut current_index = 0;

        for node in &self.nodes {
            if current_index > stop {
                break;
            }

            let entries = node.data.to_vec();
            let node_len = entries.len();

            if current_index + node_len > start {
                for (i, entry) in entries.iter().enumerate() {
                    let overall_index = current_index + i;
                    if overall_index >= start && overall_index <= stop {
                        result.push(entry.clone());
                    }
                    if overall_index >= stop {
                        break;
                    }
                }
            }
            current_index += node_len;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_quicklist() {
        let ql = QuickList::new(10, 2);
        assert_eq!(ql.llen(), 0);
        assert_eq!(ql.nodes.len(), 0);
    }

    /// Tests that rpush creates a new node when the tail is full.
    #[test]
    fn test_rpush_and_node_creation() {
        let mut ql = QuickList::new(2, 0); // Max 2 entries per node.

        ql.rpush(Bytes::from("a"));
        ql.rpush(Bytes::from("b"));
        assert_eq!(ql.llen(), 2);
        assert_eq!(ql.nodes.len(), 1);
        assert_eq!(ql.nodes[0].entry_count, 2); // Node is now full.

        // This push should create a new node, not split the old one.
        ql.rpush(Bytes::from("c"));
        assert_eq!(ql.llen(), 3);
        assert_eq!(ql.nodes.len(), 2, "A new node should have been created");

        // The original node should remain full and untouched.
        assert_eq!(ql.nodes[0].entry_count, 2, "Original node should still have 2 entries");
        // The new tail node should contain the new element.
        assert_eq!(ql.nodes[1].entry_count, 1, "New node should have 1 entry");

        let range = ql.lrange(0, -1);
        let vec: Vec<&str> = range.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec, vec!["a", "b", "c"]);
    }

    /// Tests that lpush creates a new node when the head is full.
    #[test]
    fn test_lpush_and_node_creation() {
        let mut ql = QuickList::new(2, 0);

        ql.lpush(Bytes::from("a"));
        ql.lpush(Bytes::from("b")); // Node is now full with ["b", "a"]
        assert_eq!(ql.llen(), 2);
        assert_eq!(ql.nodes.len(), 1);

        // This push should create a new head node.
        ql.lpush(Bytes::from("c"));
        assert_eq!(ql.llen(), 3);
        assert_eq!(ql.nodes.len(), 2, "A new node should have been created");

        // The new head node should contain "c".
        assert_eq!(ql.nodes[0].entry_count, 1);
        // The old node should remain untouched at the second position.
        assert_eq!(ql.nodes[1].entry_count, 2);

        let range = ql.lrange(0, -1);
        let vec: Vec<&str> = range.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec, vec!["c", "b", "a"]);
    }

    /// Tests popping from the right, accounting for the new push logic.
    #[test]
    fn test_rpop() {
        let mut ql = QuickList::new(2, 0);
        ql.rpush(Bytes::from("a"));
        ql.rpush(Bytes::from("b"));
        ql.rpush(Bytes::from("c")); // State with new logic: Node["a", "b"], Node["c"]

        assert_eq!(ql.rpop(), Some(Bytes::from("c")));
        assert_eq!(ql.llen(), 2);
        assert_eq!(ql.nodes.len(), 1, "The node containing 'c' should have been removed");

        assert_eq!(ql.rpop(), Some(Bytes::from("b")));
        assert_eq!(ql.llen(), 1);
        assert_eq!(ql.nodes.len(), 1, "The node should not be empty yet");

        assert_eq!(ql.rpop(), Some(Bytes::from("a")));
        assert_eq!(ql.llen(), 0);
        assert_eq!(ql.nodes.len(), 0, "Final node should be gone");

        assert_eq!(ql.rpop(), None);
    }

    /// Tests popping from the left, accounting for the new push logic.
    #[test]
    fn test_lpop() {
        let mut ql = QuickList::new(2, 0);
        ql.rpush(Bytes::from("a"));
        ql.rpush(Bytes::from("b"));
        ql.rpush(Bytes::from("c")); // State with new logic: Node["a", "b"], Node["c"]

        assert_eq!(ql.lpop(), Some(Bytes::from("a")));
        assert_eq!(ql.llen(), 2);
        assert_eq!(ql.nodes.len(), 2, "Node should not be empty yet");

        assert_eq!(ql.lpop(), Some(Bytes::from("b")));
        assert_eq!(ql.llen(), 1);
        assert_eq!(ql.nodes.len(), 1, "The node containing 'a,b' should now be removed");

        assert_eq!(ql.lpop(), Some(Bytes::from("c")));
        assert_eq!(ql.llen(), 0);

        assert_eq!(ql.lpop(), None);
    }

    /// Tests lrange with the new node creation logic.
    #[test]
    fn test_lrange() {
        let mut ql = QuickList::new(2, 0);
        ql.rpush(Bytes::from("a"));
        ql.rpush(Bytes::from("b")); // Node ["a", "b"]
        ql.rpush(Bytes::from("c")); // Node ["c"]
        ql.rpush(Bytes::from("d")); // Node ["c", "d"]
        ql.rpush(Bytes::from("e")); // Node ["e"]
        // Final State: Node["a", "b"], Node["c", "d"], Node["e"]

        // Test full range
        let range_all = ql.lrange(0, -1);
        assert_eq!(range_all.len(), 5);
        assert_eq!(range_all[4], Bytes::from("e"));

        // Test sub-range spanning nodes
        let range_sub = ql.lrange(1, 3);
        let vec_sub: Vec<&str> =
            range_sub.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec_sub, vec!["b", "c", "d"]);

        // Test negative indices
        let range_neg = ql.lrange(-2, -1);
        let vec_neg: Vec<&str> =
            range_neg.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec_neg, vec!["d", "e"]);
    }

    /// Tests a mixed sequence of operations with the new logic.
    #[test]
    fn test_mixed_operations() {
        let mut ql = QuickList::new(3, 0);

        ql.rpush(Bytes::from("1"));
        ql.rpush(Bytes::from("2"));
        ql.lpush(Bytes::from("0")); // State: ["0", "1", "2"]
        assert_eq!(ql.llen(), 3);
        assert_eq!(ql.nodes.len(), 1);

        assert_eq!(ql.lpop(), Some(Bytes::from("0"))); // State: ["1", "2"]
        assert_eq!(ql.llen(), 2);

        ql.rpush(Bytes::from("3")); // Node is now full: ["1", "2", "3"]
        ql.rpush(Bytes::from("4")); // New node created: Node["1","2","3"], Node["4"]
        assert_eq!(ql.llen(), 4);
        assert_eq!(ql.nodes.len(), 2);

        assert_eq!(ql.rpop(), Some(Bytes::from("4")));
        assert_eq!(ql.rpop(), Some(Bytes::from("3")));
        assert_eq!(ql.rpop(), Some(Bytes::from("2")));
        assert_eq!(ql.rpop(), Some(Bytes::from("1")));
        assert_eq!(ql.llen(), 0);
        assert_eq!(ql.nodes.len(), 0);
    }
}
