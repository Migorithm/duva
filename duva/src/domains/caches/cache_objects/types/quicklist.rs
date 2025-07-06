use bytes::Bytes;
use std::collections::VecDeque;
// For a true Redis implementation, a crate like 'lzf' would be used.
use lzf;

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

    /// Removes and returns the last entry.
    fn rpop(&mut self) -> Option<Bytes> {
        if self.data.is_empty() {
            return None;
        }
        let mut cursor = 0;
        let mut last_entry_start = 0;
        while cursor < self.data.len() {
            let entry_start = cursor;
            if let Ok(len_bytes) = self.data[cursor..cursor + 4].try_into() {
                let len = u32::from_le_bytes(len_bytes) as usize;
                cursor += 4 + len;
                if cursor <= self.data.len() {
                    last_entry_start = entry_start;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if let Ok(len_bytes) = self.data[last_entry_start..last_entry_start + 4].try_into() {
            let len = u32::from_le_bytes(len_bytes) as usize;
            let entry_data = Bytes::copy_from_slice(
                &self.data[last_entry_start + 4..last_entry_start + 4 + len],
            );
            self.data.truncate(last_entry_start);
            Some(entry_data)
        } else {
            None
        }
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
                    break;
                }
            } else {
                break;
            }
        }
        entries
    }

    /// Returns the total memory size of the ziplist.
    fn byte_size(&self) -> usize {
        self.data.len()
    }
}

#[derive(Debug)]
enum NodeData {
    Uncompressed(Ziplist),
    Compressed(Vec<u8>),
}

/// Represents a single node in the QuickList, which contains a Ziplist.
#[derive(Debug)]
struct QuickListNode {
    /// The actual data, either uncompressed (Ziplist) or compressed.
    data: NodeData,
    /// Number of entries in the ziplist.
    entry_count: usize,
    /// Max size setting. Positive for entry count, negative for bytes.
    fill_factor: isize,
}

impl QuickListNode {
    fn new(fill_factor: isize) -> Self {
        Self { data: NodeData::Uncompressed(Ziplist::default()), entry_count: 0, fill_factor }
    }

    /// Ensures the node is decompressed. Must be called before any read/write.
    fn ensure_decompressed(&mut self) {
        let current_data =
            std::mem::replace(&mut self.data, NodeData::Uncompressed(Ziplist::default()));
        self.data = match current_data {
            | NodeData::Compressed(bytes) => {
                // Provide a sane upper limit for the decompressed size.
                // We'll use the fill_factor plus a little buffer.
                let max_size = if self.fill_factor < 0 {
                    (-self.fill_factor as usize) * 1024 + 1024 // e.g., 1KB limit becomes 2KB buffer
                } else {
                    // If based on count, use a reasonable default like 64KB
                    65536
                };

                let decompressed = lzf::decompress(&bytes, max_size).unwrap_or_default();
                NodeData::Uncompressed(Ziplist { data: decompressed })
            },
            | uncompressed => uncompressed,
        };
    }

    /// Attempts to compress the node.
    fn try_compress(&mut self) {
        // Temporarily take ownership of data to work on it
        let current_data =
            std::mem::replace(&mut self.data, NodeData::Uncompressed(Ziplist::default()));
        self.data = match current_data {
            | NodeData::Uncompressed(ziplist) => {
                if ziplist.byte_size() > 48 {
                    // Don't compress tiny nodes
                    let compressed = lzf::compress(&ziplist.data).unwrap_or_default();
                    if compressed.len() < ziplist.byte_size() {
                        NodeData::Compressed(compressed)
                    } else {
                        NodeData::Uncompressed(ziplist) // Not worth it
                    }
                } else {
                    NodeData::Uncompressed(ziplist) // Too small
                }
            },
            | compressed => compressed, // Already compressed
        };
    }
    fn byte_size(&self) -> usize {
        match &self.data {
            | NodeData::Uncompressed(ziplist) => ziplist.byte_size(),
            | NodeData::Compressed(bytes) => bytes.len(),
        }
    }

    fn is_full(&mut self, new_value_size: usize) -> bool {
        self.ensure_decompressed();
        if self.fill_factor > 0 {
            self.entry_count >= self.fill_factor as usize
        } else {
            let max_bytes = (-self.fill_factor) as usize * 1024;
            self.byte_size() + new_value_size + 4 > max_bytes
        }
    }

    fn lpush(&mut self, value: Bytes) {
        self.ensure_decompressed();
        if let NodeData::Uncompressed(ziplist) = &mut self.data {
            ziplist.lpush(&value);
            self.entry_count += 1;
        }
    }

    fn rpush(&mut self, value: Bytes) {
        self.ensure_decompressed();
        if let NodeData::Uncompressed(ziplist) = &mut self.data {
            ziplist.push_back(&value);
            self.entry_count += 1;
        }
    }

    /// Pops a value from the front or back.
    fn lpop(&mut self) -> Option<Bytes> {
        self.ensure_decompressed();
        if let NodeData::Uncompressed(ziplist) = &mut self.data {
            let res = ziplist.lpop()?;
            self.entry_count -= 1;
            return Some(res);
        }
        None
    }

    fn rpop(&mut self) -> Option<Bytes> {
        self.ensure_decompressed();
        if let NodeData::Uncompressed(ziplist) = &mut self.data {
            let res = ziplist.rpop()?;
            self.entry_count -= 1;
            return Some(res);
        }
        None
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
    node_pool: Vec<QuickListNode>,
}

impl QuickList {
    pub fn new(fill_factor: isize, compress_depth: usize) -> Self {
        Self {
            nodes: VecDeque::new(),
            len: 0,
            fill_factor,
            compress_depth,
            node_pool: Vec::with_capacity(16), // Pre-allocate pool
        }
    }

    // ## Private Helpers for Pooling and Merging
    fn get_node(&mut self) -> QuickListNode {
        self.node_pool.pop().unwrap_or_else(|| QuickListNode::new(self.fill_factor))
    }

    fn return_node(&mut self, mut node: QuickListNode) {
        if self.node_pool.len() < 16 {
            // Limit pool size
            node.data = NodeData::Uncompressed(Ziplist::default());
            node.entry_count = 0;
            self.node_pool.push(node);
        }
    }

    fn try_merge_at(&mut self, index: usize) {
        if self.fill_factor > 0 {
            return;
        }

        if index >= self.nodes.len() - 1 {
            return;
        }

        let should_merge = {
            let node = &self.nodes[index];
            let next = &self.nodes[index + 1];
            let max_bytes = (-self.fill_factor) as usize * 1024;

            // Define a mergeable size, for example, 25% of the max node size.
            // This check prevents merging two almost-full nodes.
            let merge_threshold = max_bytes / 4;

            (node.entry_count > 0 && next.entry_count > 0)
                && (node.byte_size() < merge_threshold || next.byte_size() < merge_threshold)
                && (node.byte_size() + next.byte_size() < max_bytes)
        };

        if should_merge {
            let mut next_node = self.nodes.remove(index + 1).unwrap();
            next_node.ensure_decompressed();

            let node = &mut self.nodes[index];
            node.ensure_decompressed();

            if let NodeData::Uncompressed(ziplist) = &mut node.data
                && let NodeData::Uncompressed(next_ziplist) = &mut next_node.data
            {
                ziplist.data.extend_from_slice(&next_ziplist.data);
                node.entry_count += next_node.entry_count;
            }
            self.return_node(next_node);
        }
    }

    pub fn llen(&self) -> usize {
        self.len
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
        let mut new_node = self.get_node();
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
        let mut new_node = self.get_node();
        new_node.rpush(value);
        self.nodes.push_back(new_node);
        self.len += 1;
    }

    /// Pops a value from the front (left) of the list.
    pub fn lpop(&mut self) -> Option<Bytes> {
        let val = self.nodes.front_mut()?.lpop();
        if val.is_some() {
            self.len -= 1;
            if self.nodes.front().unwrap().entry_count == 0 {
                let node = self.nodes.pop_front().unwrap();
                self.return_node(node);
            }

            // try to merge only if there are at least two nodes.
            // safety guard to ensure we only attempt a merge when there are actually two nodes available to combine.
            if self.nodes.len() >= 2 {
                self.try_merge_at(0);
            }
        }
        val
    }
    /// Pops a value from the back (right) of the list.
    pub fn rpop(&mut self) -> Option<Bytes> {
        let val = self.nodes.back_mut()?.rpop();
        if val.is_some() {
            self.len -= 1;
            if self.nodes.back().unwrap().entry_count == 0 {
                let node = self.nodes.pop_back().unwrap();
                self.return_node(node);
            }
            if self.nodes.len() >= 2 {
                self.try_merge_at(self.nodes.len() - 2); // Attempt to merge new tail with its next (which was the old tail)
            }
        }
        val
    }

    pub fn compress(&mut self) {
        let node_count = self.nodes.len();
        if self.compress_depth > 0 && node_count > self.compress_depth * 2 {
            for i in self.compress_depth..(node_count - self.compress_depth) {
                if let Some(node) = self.nodes.get_mut(i) {
                    node.try_compress();
                }
            }
        }
    }

    /// Returns a range of elements. Handles negative indices like Redis.
    pub fn lrange(&mut self, start: isize, stop: isize) -> Vec<Bytes> {
        if self.len == 0 {
            return vec![];
        }

        // 1. Calculate absolute start and end indices
        let len = self.len as isize;
        let start = if start < 0 { (len + start).max(0) } else { start } as usize;
        let stop = if stop < 0 { (len + stop).max(0) } else { stop } as usize;

        if start > stop {
            return vec![];
        }

        let mut result = Vec::with_capacity(stop - start + 1);
        let mut current_index = 0;

        // 2. Iterate through nodes to find the requested range
        for node in &mut self.nodes {
            let node_len = node.entry_count;
            if current_index + node_len <= start {
                // This node is before our range, skip it
                current_index += node_len;
                continue;
            }
            if current_index > stop {
                // We have passed our range, no need to check more nodes
                break;
            }

            // 3. Decompress the node and read its entries
            node.ensure_decompressed();
            if let NodeData::Uncompressed(ziplist) = &node.data {
                let entries = ziplist.to_vec();
                for (i, entry) in entries.iter().enumerate() {
                    let overall_index = current_index + i;
                    if overall_index >= start && overall_index <= stop {
                        // Clone the Bytes object and add it to our result.
                        // Cloning is cheap (reference counted).
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
        assert_eq!(ql.fill_factor, 10);
    }

    #[test]
    fn test_node_creation() {
        // Test with entry-count limit
        let mut ql_count = QuickList::new(2, 0);
        ql_count.rpush(Bytes::from("a"));
        ql_count.rpush(Bytes::from("b")); // Node is full
        assert_eq!(ql_count.nodes.len(), 1);
        ql_count.rpush(Bytes::from("c")); // MUST create a new node
        assert_eq!(ql_count.nodes.len(), 2);

        // Test with size limit
        let mut ql_size = QuickList::new(-1, 0); // 1KB limit
        let large_string = Bytes::from(vec![0; 1020]); // Fills node exactly after 4-byte overhead
        ql_size.rpush(large_string);
        assert_eq!(ql_size.nodes.len(), 1);
        ql_size.rpush(Bytes::from("a")); // MUST create a new node
        assert_eq!(ql_size.nodes.len(), 2);
    }

    #[test]
    fn test_node_creation_with_size_limit() {
        let mut ql = QuickList::new(-1, 0); // Use 1KB size limit (1024 bytes)

        // Create a payload that will leave the node *almost* full.
        // 1024 (limit) - 4 (len prefix) - 5 (space for next push: "a" + len prefix) = 1015 bytes
        let almost_full_string = Bytes::from(vec![0; 1015]);
        ql.rpush(almost_full_string);

        assert_eq!(ql.nodes.len(), 1);
        assert_eq!(ql.nodes[0].byte_size(), 1015 + 4); // 1019 bytes

        // This next push should still fit.
        ql.rpush(Bytes::from("a"));
        assert_eq!(ql.nodes.len(), 1, "The node should not be full yet");
        assert_eq!(ql.nodes[0].byte_size(), 1019 + 1 + 4); // 1024 bytes

        // The node is now exactly full. Pushing another item MUST create a new node.
        ql.rpush(Bytes::from("b"));
        assert_eq!(ql.nodes.len(), 2, "A new node should have been created");
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

    #[test]
    fn test_compression_and_decompression_on_access() {
        let mut ql = QuickList::new(-1, 1); // compress_depth = 1

        // Create 3 full nodes
        for _ in 0..3 {
            ql.rpush(Bytes::from(vec![0; 1025]));
        }
        assert_eq!(ql.nodes.len(), 3);

        ql.compress();

        // Verify the middle node is compressed
        assert!(matches!(ql.nodes[1].data, NodeData::Compressed(_)));

        // LRANGE should still work, forcing decompression.
        let range = ql.lrange(1, 1);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].len(), 1025);

        // After access, the node should be uncompressed again.
        assert!(matches!(ql.nodes[1].data, NodeData::Uncompressed(_)));
    }

    #[test]
    fn test_compress_respects_depth() {
        let mut ql = QuickList::new(-1, 1); // compress_depth = 1

        // Create 3 nodes. Total nodes (3) is NOT > compress_depth * 2 (2). So no compression.
        ql.rpush(Bytes::from(vec![0; 1025]));
        ql.rpush(Bytes::from(vec![0; 1025]));
        assert_eq!(ql.nodes.len(), 2);

        ql.compress();
        // No nodes should be compressed because the list isn't long enough
        assert!(matches!(ql.nodes[0].data, NodeData::Uncompressed(_)));
        assert!(matches!(ql.nodes[1].data, NodeData::Uncompressed(_)));
    }

    #[test]
    fn test_compression() {
        let mut ql = QuickList::new(-1, 1); // 4KB nodes, compress depth of 1

        // Create 3 nodes worth of data
        for i in 0..100 {
            let s = format!("long_string_to_ensure_compression_{}", i);
            ql.rpush(Bytes::from(s));
        }
        for i in 0..100 {
            let s = format!("long_string_to_ensure_compression_{}", i);
            ql.rpush(Bytes::from(s));
        }
        for i in 0..100 {
            let s = format!("long_string_to_ensure_compression_{}", i);
            ql.rpush(Bytes::from(s));
        }
        assert!(ql.nodes.len() >= 3, "Should have at least 3 nodes to test compression");

        // Compress the list
        ql.compress();

        // The middle node should be compressed, head and tail should not
        let middle_node_index = ql.nodes.len() / 2;
        assert!(matches!(ql.nodes[0].data, NodeData::Uncompressed(_)));
        assert!(matches!(ql.nodes[middle_node_index].data, NodeData::Compressed(_)));
        assert!(matches!(ql.nodes.back().unwrap().data, NodeData::Uncompressed(_)));

        // Verify operations still work on a compressed list
        assert_eq!(ql.llen(), 300);
        let first = ql.lpop().unwrap();
        assert_eq!(first, Bytes::from("long_string_to_ensure_compression_0"));
        assert_eq!(ql.llen(), 299);
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

    #[test]
    fn test_node_pool_recycling() {
        let mut ql = QuickList::new(2, 0);

        for _ in 0..5 {
            ql.rpush(Bytes::from("data"));
        }
        assert_eq!(ql.nodes.len(), 3);
        assert_eq!(ql.node_pool.len(), 0);

        for _ in 0..5 {
            ql.lpop();
        }
        assert_eq!(ql.nodes.len(), 0);
        assert_eq!(ql.node_pool.len(), 3);

        for _ in 0..3 {
            ql.rpush(Bytes::from("new data"));
        }
        assert_eq!(ql.nodes.len(), 2);
        assert_eq!(ql.node_pool.len(), 1);
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

    #[test]
    fn test_node_merging_is_correct() {
        let mut ql = QuickList::new(-1, 0); // 1KB size limit

        // 1. Create a state with two small nodes separated by a large one,
        // which forces them to be in different nodes initially.
        ql.rpush(Bytes::from("small_A"));
        ql.rpush(Bytes::from(vec![0; 1025])); // Large node
        ql.rpush(Bytes::from("small_C"));
        assert_eq!(ql.nodes.len(), 3);

        // 2. Manually remove the middle node to create the test condition: [small_A], [small_C]
        let middle_node = ql.nodes.remove(1).unwrap();
        ql.len -= middle_node.entry_count; // IMPORTANT: keep master length correct
        ql.return_node(middle_node);
        assert_eq!(ql.nodes.len(), 2);

        // 3. Trigger the merge by calling the helper directly.
        ql.try_merge_at(0);

        // 4. Assert that the merge was successful.
        assert_eq!(ql.nodes.len(), 1, "The two small nodes should have merged");
        assert_eq!(ql.llen(), 2);
        let final_content = ql.lrange(0, -1);
        assert_eq!(final_content, vec![Bytes::from("small_A"), Bytes::from("small_C")]);
    }
}
