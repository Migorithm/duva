/// QuickList Architecture
///    Node-based structure: QuickList uses multiple nodes (QuickListNode) to manage large lists
///    Compression support: Nodes can be compressed when not actively accessed, with ziplists being the uncompressed format
///    Fill factor management: Each node respects size or count limits, with ziplists handling the actual data storag
use bytes::Bytes;
use lzf;
use std::collections::VecDeque;

use crate::make_smart_pointer;

#[derive(Debug, Clone, PartialEq, Eq, Default, bincode::Decode, bincode::Encode)]
struct Ziplist(Vec<u8>);

make_smart_pointer!(Ziplist, Vec<u8>);

impl Ziplist {
    /// Efficiently prepends an entry by rebuilding the data vector once.
    fn lpush(&mut self, value: &Bytes) {
        let mut new_data = Vec::with_capacity(value.len() + 4 + self.len());
        new_data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        new_data.extend_from_slice(value);
        new_data.extend_from_slice(self);
        self.0 = new_data;
    }

    /// Removes and returns the first entry.
    fn lpop(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let len = u32::from_le_bytes(self[0..4].try_into().ok()?) as usize;
        let entry_data = Bytes::copy_from_slice(&self[4..4 + len]);

        // Removes the first entry by shifting memory in-place, avoiding re-allocation.
        let total_entry_len = 4 + len;
        let remaining_len = self.len() - total_entry_len;
        self.copy_within(total_entry_len.., 0);
        self.truncate(remaining_len);

        Some(entry_data)
    }

    /// Appends an entry to the end of the ziplist.
    fn rpush(&mut self, value: &Bytes) {
        self.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.extend_from_slice(value);
    }

    /// Removes and returns the last entry. This remains an O(N) scan.
    fn rpop(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let mut cursor = 0;
        let mut last_entry_start = 0;
        while cursor < self.len() {
            let entry_start = cursor;
            if let Ok(len_bytes) = self[cursor..cursor + 4].try_into() {
                let len = u32::from_le_bytes(len_bytes) as usize;
                cursor += 4 + len;
                if cursor <= self.len() {
                    last_entry_start = entry_start;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if let Ok(len_bytes) = self[last_entry_start..last_entry_start + 4].try_into() {
            let len = u32::from_le_bytes(len_bytes) as usize;
            let entry_data =
                Bytes::copy_from_slice(&self[last_entry_start + 4..last_entry_start + 4 + len]);
            self.truncate(last_entry_start);
            Some(entry_data)
        } else {
            None
        }
    }

    /// Decodes the ziplist back into a vector of Bytes.
    fn to_vec(&self) -> Vec<Bytes> {
        let mut entries = Vec::new();
        let mut cursor = 0;
        while cursor < self.len() {
            if let Ok(len_bytes) = self[cursor..(cursor + 4)].try_into() {
                let len = u32::from_le_bytes(len_bytes) as usize;
                cursor += 4;
                if cursor + len <= self.len() {
                    entries.push(Bytes::copy_from_slice(&self[cursor..cursor + len]));
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
}

#[derive(Debug, PartialEq, Eq, Clone, bincode::Decode, bincode::Encode)]
enum NodeData {
    Uncompressed(Ziplist),
    Compressed(Vec<u8>),
}

impl Default for NodeData {
    fn default() -> Self {
        Self::Uncompressed(Ziplist::default())
    }
}

/// Represents a single node in the QuickList, which contains a Ziplist.
#[derive(Debug, PartialEq, Eq, Clone, Default, bincode::Decode, bincode::Encode)]
struct QuickListNode {
    /// The actual data, either uncompressed (Ziplist) or compressed.
    data: NodeData,
    /// Number of entries in the ziplist.
    entry_count: usize,
}

impl QuickListNode {
    /// Ensures the node is decompressed. Must be called before any read/write.
    fn ensure_decompressed(&mut self, fill_factor: &FillFactor) {
        let current_data =
            std::mem::replace(&mut self.data, NodeData::Uncompressed(Ziplist::default()));
        self.data = match current_data {
            | NodeData::Compressed(bytes) => {
                let max_size = match fill_factor {
                    | FillFactor::Size(kb) => kb * 1024 + 1024, // Add a safety buffer
                    | FillFactor::Count(_) => 65536, // Reasonable default for count-based
                };

                let decompressed = lzf::decompress(&bytes, max_size).unwrap_or_default();
                NodeData::Uncompressed(Ziplist(decompressed))
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
                if ziplist.len() > 48 {
                    // Don't compress tiny nodes
                    let compressed = lzf::compress(&ziplist).unwrap_or_default();
                    if compressed.len() < ziplist.len() {
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
            | NodeData::Uncompressed(ziplist) => ziplist.len(),
            | NodeData::Compressed(bytes) => bytes.len(),
        }
    }

    fn is_full(&mut self, new_value_size: usize, fill_factor: &FillFactor) -> bool {
        self.ensure_decompressed(fill_factor);
        match fill_factor {
            | FillFactor::Count(max_entries) => self.entry_count >= *max_entries,
            | FillFactor::Size(kb) => {
                let max_bytes = kb * 1024;
                self.byte_size() + new_value_size + 4 > max_bytes
            },
        }
    }

    fn lpush(&mut self, value: Bytes, fill_factor: &FillFactor) {
        self.ensure_decompressed(fill_factor);
        if let NodeData::Uncompressed(ziplist) = &mut self.data {
            ziplist.lpush(&value);
            self.entry_count += 1;
        }
    }

    fn rpush(&mut self, value: Bytes, fill_factor: &FillFactor) {
        self.ensure_decompressed(fill_factor);
        if let NodeData::Uncompressed(ziplist) = &mut self.data {
            ziplist.rpush(&value);
            self.entry_count += 1;
        }
    }

    /// Pops a value from the front or back.
    fn lpop(&mut self, fill_factor: &FillFactor) -> Option<Bytes> {
        self.ensure_decompressed(fill_factor);
        let NodeData::Uncompressed(ziplist) = &mut self.data else {
            return None;
        };
        let res = ziplist.lpop()?;
        self.entry_count -= 1;
        Some(res)
    }

    fn rpop(&mut self, fill_factor: &FillFactor) -> Option<Bytes> {
        self.ensure_decompressed(fill_factor);
        let NodeData::Uncompressed(ziplist) = &mut self.data else {
            return None;
        };
        let res = ziplist.rpop()?;
        self.entry_count -= 1;
        Some(res)
    }
}
/// A memory-optimized list structure, similar to Redis's Quicklist.
#[derive(Debug, PartialEq, Eq, Clone, Default, bincode::Decode, bincode::Encode)]
pub struct QuickList {
    nodes: VecDeque<QuickListNode>,
    len: usize,
    fill_factor: FillFactor,
    compress_depth: usize, // Head-tail nodes to keep uncompressed. 0 to disable compression.
    node_pool: Vec<QuickListNode>,
}

/// How Many Records Does a Ziplist Contain?
/// 1. Count-based FillFactor
/// ```rust,ignore
/// FillFactor::Count(max_entries)
/// ```
///    Each ziplist contains exactly max_entries records before creating a new node
///    Example: FillFactor::Count(2) means each ziplist holds 2 records maximum
///
/// 2. Size-based FillFactor
/// ```rust,ignore
/// FillFactor::Size(kb)
/// ```
///    Each ziplist contains records until it reaches the size limit
///    The actual number varies based on record sizes
///    Example: FillFactor::Size(1) means each ziplist can hold up to 1KB of data
///
/// 3. Typical Usage
///    From the tests, we can see typical configurations:
///    Small lists: 2-3 records per ziplist for testing
///    Production: Likely larger counts (e.g., 32-64 records) or size-based limits (e.g., 8KB per ziplist)
#[derive(Debug, Copy, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum FillFactor {
    Count(usize),
    Size(usize),
}

impl Default for FillFactor {
    fn default() -> Self {
        Self::Size(8)
    }
}

impl QuickList {
    pub fn new(fill_factor: FillFactor, compress_depth: usize) -> Self {
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
        self.node_pool.pop().unwrap_or_default()
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
        let FillFactor::Size(kb) = self.fill_factor else {
            return; // Do not merge for count-based lists.
        };

        if index >= self.nodes.len() - 1 {
            return;
        }

        let max_bytes = kb * 1024;

        let should_merge = {
            let node = &self.nodes[index];
            let next = &self.nodes[index + 1];
            let merge_threshold = max_bytes / 4;

            (node.entry_count > 0 && next.entry_count > 0)
                && (node.byte_size() < merge_threshold || next.byte_size() < merge_threshold)
                && (node.byte_size() + next.byte_size()) < max_bytes
        };

        if should_merge {
            // First, remove the next node from the list.
            let mut removed_node = self.nodes.remove(index + 1).unwrap();
            removed_node.ensure_decompressed(&self.fill_factor);

            // Now, get mutable access to the current node to merge into.
            let current_node = &mut self.nodes[index];
            current_node.ensure_decompressed(&self.fill_factor);

            // Use pattern matching to get the ziplists and perform the merge.
            if let (
                NodeData::Uncompressed(current_ziplist),
                NodeData::Uncompressed(removed_ziplist),
            ) = (&mut current_node.data, &mut removed_node.data)
            {
                current_ziplist.extend_from_slice(removed_ziplist);
                current_node.entry_count += removed_node.entry_count;
            }

            self.return_node(removed_node);
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
            if !head.is_full(val_size, &self.fill_factor) {
                head.lpush(value, &self.fill_factor);
                self.len += 1;
                return;
            }
        }

        // Case 2: The list is empty OR the head node is full.
        // In both scenarios, create a new node at the front for the new value.
        let mut new_node = self.get_node();
        new_node.lpush(value, &self.fill_factor);
        self.nodes.push_front(new_node);
        self.len += 1;
    }

    /// Pushes a value to the back (right) of the list using a fast, iterative approach.
    pub fn rpush(&mut self, value: Bytes) {
        let val_size = value.len();

        // Case 1: The tail node exists and is not full.
        if let Some(tail) = self.nodes.back_mut() {
            if !tail.is_full(val_size, &self.fill_factor) {
                tail.rpush(value, &self.fill_factor);
                self.len += 1;
                return;
            }
        }

        // Case 2: The list is empty OR the tail node is full.
        // In both scenarios, create a new node at the back for the new value.
        let mut new_node = self.get_node();
        new_node.rpush(value, &self.fill_factor);
        self.nodes.push_back(new_node);
        self.len += 1;
    }

    /// Pops a value from the front (left) of the list.
    pub fn lpop(&mut self) -> Option<Bytes> {
        let val = self.nodes.front_mut()?.lpop(&self.fill_factor)?;

        self.len -= 1;
        if self.nodes.front()?.entry_count == 0 {
            let node = self.nodes.pop_front()?;
            self.return_node(node);
        }
        // try to merge only if there are at least two nodes.
        // safety guard to ensure we only attempt a merge when there are actually two nodes available to combine.
        if self.nodes.len() >= 2 {
            self.try_merge_at(0);
        }
        Some(val)
    }
    /// Pops a value from the back (right) of the list.
    pub fn rpop(&mut self) -> Option<Bytes> {
        let val = self.nodes.back_mut()?.rpop(&self.fill_factor)?;
        self.len -= 1;
        if self.nodes.back()?.entry_count == 0 {
            let node = self.nodes.pop_back()?;
            self.return_node(node);
        }
        if self.nodes.len() >= 2 {
            self.try_merge_at(self.nodes.len() - 2); // Attempt to merge new tail with its next (which was the old tail)
        }

        Some(val)
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
            node.ensure_decompressed(&self.fill_factor);
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

    pub(crate) fn ltrim(&mut self, start: isize, end: isize) {
        // Calculate absolute start and end indices
        let len = self.len as isize;
        let start = if start < 0 { (len + start).max(0) } else { start } as usize;
        let end = if end < 0 { (len + end).max(0) } else { end } as usize;

        // Handle invalid ranges
        if start > end || start >= self.len {
            // Clear the entire list
            self.nodes.clear();
            self.len = 0;
            return;
        }

        // Ensure end doesn't exceed list length
        let end = end.min(self.len - 1);

        // If the range covers the entire list, no trimming needed
        if start == 0 && end == self.len - 1 {
            return;
        }

        // Collect elements to keep
        let elements_to_keep = self.lrange(start as isize, end as isize);

        // Clear the current list
        self.nodes.clear();
        self.len = 0;

        // Rebuild the list with only the kept elements
        for element in elements_to_keep {
            self.rpush(element);
        }
    }

    pub(crate) fn lindex(&mut self, index: isize) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }

        // Calculate absolute index
        let len = self.len as isize;
        let index = if index < 0 { (len + index).max(0) } else { index } as usize;

        if index >= self.len {
            return None; // Out of bounds
        }

        let mut current_index = 0;

        // Iterate through nodes to find the requested index
        for node in &mut self.nodes {
            let node_len = node.entry_count;
            if current_index + node_len <= index {
                // This node is before our index, skip it
                current_index += node_len;
                continue;
            }

            // Decompress the node and read its entries
            node.ensure_decompressed(&self.fill_factor);
            if let NodeData::Uncompressed(ziplist) = &node.data {
                let entries = ziplist.to_vec();
                if let Some(entry) = entries.get(index - current_index) {
                    return Some(entry.clone());
                }
            }
            break; // No need to check further nodes
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_quicklist() {
        let ql = QuickList::new(FillFactor::Size(10), 2);
        assert_eq!(ql.llen(), 0);
        assert_eq!(ql.nodes.len(), 0);
        assert_eq!(ql.fill_factor, FillFactor::Size(10));
    }

    #[test]
    fn test_node_creation() {
        // Test with entry-count limit
        // CHANGED: Use the FillFactor enum
        let mut ql_count = QuickList::new(FillFactor::Count(2), 0);
        ql_count.rpush(Bytes::from("a"));
        ql_count.rpush(Bytes::from("b")); // Node is full
        assert_eq!(ql_count.nodes.len(), 1);
        ql_count.rpush(Bytes::from("c")); // MUST create a new node
        assert_eq!(ql_count.nodes.len(), 2);

        // Test with size limit
        // CHANGED: Use the FillFactor enum
        let mut ql_size = QuickList::new(FillFactor::Size(1), 0); // 1KB limit
        let large_string = Bytes::from(vec![0; 1020]); // Fills node exactly after 4-byte overhead
        ql_size.rpush(large_string);
        assert_eq!(ql_size.nodes.len(), 1);
        ql_size.rpush(Bytes::from("a")); // MUST create a new node
        assert_eq!(ql_size.nodes.len(), 2);
    }

    #[test]
    fn test_node_creation_with_size_limit() {
        let mut ql = QuickList::new(FillFactor::Size(1), 0); // Use 1KB size limit (1024 bytes)

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
        let mut ql = QuickList::new(FillFactor::Count(2), 0); // Max 2 entries per node.

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
        let mut ql = QuickList::new(FillFactor::Count(2), 0);

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
        let mut ql = QuickList::new(FillFactor::Size(1), 1); // compress_depth = 1

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
        let mut ql = QuickList::new(FillFactor::Size(1), 1); // compress_depth = 1

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
        let mut ql = QuickList::new(FillFactor::Size(1), 1);

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
        let mut ql = QuickList::new(FillFactor::Count(2), 0);
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
        let mut ql = QuickList::new(FillFactor::Count(2), 0);

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
        let mut ql = QuickList::new(FillFactor::Count(2), 0);
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
        let mut ql = QuickList::new(FillFactor::Count(2), 0);
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
        let mut ql = QuickList::new(FillFactor::Count(3), 0);

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
        let mut ql = QuickList::new(FillFactor::Size(1), 0); // 1KB size limit

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

    #[test]
    fn test_ltrim_basic() {
        let mut ql = QuickList::new(FillFactor::Count(2), 0);

        // Create list: ["1", "2", "3"]
        ql.rpush(Bytes::from("1"));
        ql.rpush(Bytes::from("2"));
        ql.rpush(Bytes::from("3"));
        assert_eq!(ql.llen(), 3);

        // Trim to keep elements from index 1 to end (should keep ["2", "3"])
        ql.ltrim(1, -1);
        assert_eq!(ql.llen(), 2);

        let range = ql.lrange(0, -1);
        let vec: Vec<&str> = range.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec, vec!["2", "3"]);
    }

    #[test]
    fn test_ltrim_negative_indices() {
        let mut ql = QuickList::new(FillFactor::Count(2), 0);

        // Create list: ["a", "b", "c", "d", "e"]
        for c in ['a', 'b', 'c', 'd', 'e'] {
            ql.rpush(Bytes::from(c.to_string()));
        }
        assert_eq!(ql.llen(), 5);

        // Trim to keep elements from index 1 to -2 (should keep ["b", "c", "d"])
        ql.ltrim(1, -2);
        assert_eq!(ql.llen(), 3);

        let range = ql.lrange(0, -1);
        let vec: Vec<&str> = range.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec, vec!["b", "c", "d"]);
    }

    #[test]
    fn test_ltrim_empty_result() {
        let mut ql = QuickList::new(FillFactor::Count(2), 0);

        // Create list: ["1", "2", "3"]
        ql.rpush(Bytes::from("1"));
        ql.rpush(Bytes::from("2"));
        ql.rpush(Bytes::from("3"));
        assert_eq!(ql.llen(), 3);

        // Trim with invalid range (start > end)
        ql.ltrim(5, 1);
        assert_eq!(ql.llen(), 0);
        assert_eq!(ql.nodes.len(), 0);
    }

    #[test]
    fn test_ltrim_no_change() {
        let mut ql = QuickList::new(FillFactor::Count(2), 0);

        // Create list: ["1", "2", "3"]
        ql.rpush(Bytes::from("1"));
        ql.rpush(Bytes::from("2"));
        ql.rpush(Bytes::from("3"));
        assert_eq!(ql.llen(), 3);

        // Trim to keep entire list (should not change anything)
        ql.ltrim(0, -1);
        assert_eq!(ql.llen(), 3);

        let range = ql.lrange(0, -1);
        let vec: Vec<&str> = range.iter().map(|b| std::str::from_utf8(b).unwrap()).collect();
        assert_eq!(vec, vec!["1", "2", "3"]);
    }
}
