use crate::domains::query_io::SERDE_CONFIG;
use crate::domains::query_io::serialized_len_with_bincode;
use crate::domains::replications::LogEntry;
use crate::domains::replications::WriteOperation;
use crate::domains::replications::interfaces::TWriteAheadLog;
use anyhow::Result;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Write;
use std::io::{BufReader, Seek, SeekFrom};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

const SEGMENT_SIZE: usize = 1024 * 1024; // 1MB per segment
const WRITE_OP_PREFIX: char = '#';

/// A local write-ahead-log (WAL) file (op_logs) implementation using segmented logs.
pub struct FileOpLogs {
    path: PathBuf,
    active_segment: Segment,
    segments: Vec<Segment>,
}

#[derive(Debug)]
struct Segment {
    path: PathBuf,
    start_index: u64,
    end_index: u64,
    size: usize,
    writer: BufWriter<File>,

    // * In-memory cache for the index. Maps log_index to byte_offset in the data file.
    // ! For very large logs, this might need optimization
    // !(e.g., sparse index, memory-mapped index files).
    lookups: Vec<LookupIndex>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LookupIndex {
    log_index: u64,
    byte_offset: usize,
}
impl LookupIndex {
    fn new(log_index: u64, byte_offset: usize) -> Self {
        Self { log_index, byte_offset }
    }
}

impl Segment {
    fn new(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new().create(true).read(true).append(true).open(&path)?;

        let initial_size = file.metadata()?.len() as usize;

        let writer = BufWriter::with_capacity(64 * 1024, file); // 64KB buffer for better write performance
        Ok(Self {
            path,
            writer,
            start_index: 0,
            end_index: 0,
            size: initial_size,
            lookups: Vec::new(),
        })
    }

    // Add method to read operation at specific offset
    fn read_at_offset(&mut self, offset: usize) -> Result<WriteOperation> {
        // Open a temporary, read-only file handle with larger buffer for better I/O
        let file = File::open(&self.path)?;
        let mut reader = BufReader::with_capacity(8192, file);

        // Seek to the exact starting byte of the operation.
        reader.seek(SeekFrom::Start(offset as u64))?;

        // Decode only ONE operation from the stream.
        // This is vastly more efficient than read_to_end.
        let mut prefix_buf = [0u8; 1];
        reader.read_exact(&mut prefix_buf)?;

        if prefix_buf[0] as char != WRITE_OP_PREFIX {
            return Err(anyhow::anyhow!(
                "Expected WRITE_OP_PREFIX '{}', but found '{}'",
                WRITE_OP_PREFIX,
                prefix_buf[0] as char
            ));
        }
        //Decode the `WriteOperation` directly from the stream.
        let operation: WriteOperation = bincode::decode_from_std_read(&mut reader, SERDE_CONFIG)?;
        Ok(operation)
    }

    fn from_path(path: &PathBuf) -> Result<Self> {
        // Open the file for writing and get its size.
        let file = OpenOptions::new().read(true).append(true).open(path)?;
        let file_size = file.metadata()?.len() as usize;
        let writer = BufWriter::with_capacity(64 * 1024, file); // 64KB buffer for better write performance

        // If file is empty, no need to build index
        if file_size == 0 {
            return Ok(Segment {
                path: path.clone(),
                start_index: 0,
                end_index: 0,
                size: 0,
                lookups: Vec::new(),
                writer,
            });
        }

        // Optimized index building - only read what we need
        let mut reader = File::open(path)?;
        let mut buf = Vec::with_capacity(file_size);
        reader.read_to_end(&mut buf)?;

        let operations = LogEntry::deserialize(Bytes::copy_from_slice(&buf))?;
        let (start_index, end_index) = match (operations.first(), operations.last()) {
            (Some(first), Some(last)) => (first.log_index, last.log_index),
            _ => (0, 0),
        };

        let mut current_offset = 0;
        let mut lookups = Vec::with_capacity(operations.len());

        // Build index more efficiently - single pass
        for op in operations.iter() {
            lookups.push(LookupIndex::new(op.log_index, current_offset));
            // Use the existing helper function
            current_offset += serialized_len_with_bincode(WRITE_OP_PREFIX, op);
        }

        Ok(Segment { path: path.clone(), start_index, end_index, size: file_size, lookups, writer })
    }

    fn find_offset(&self, log_index: u64) -> Option<usize> {
        self.lookups
            .binary_search_by_key(&log_index, |index| index.log_index)
            .ok()
            .map(|found_index| self.lookups[found_index].byte_offset)
    }

    fn truncate(&mut self, log_index: u64) -> Result<()> {
        // Use binary search for better performance on large segments
        let truncate_pos =
            match self.lookups.binary_search_by_key(&log_index, |lookup| lookup.log_index) {
                Ok(pos) => pos + 1, // Keep the found entry
                Err(pos) => pos,    // Insert position is where we truncate
            };

        if truncate_pos == 0 {
            // No entry with log_index <= specified one, so truncate the entire segment.
            self.lookups.clear();
            self.end_index = self.start_index;
            self.size = 0;
            self.writer.get_ref().set_len(0)?;
            self.writer.seek(SeekFrom::Start(0))?;
            return Ok(());
        }

        // Determine the new size of the file
        let new_size = if let Some(next_lookup) = self.lookups.get(truncate_pos) {
            next_lookup.byte_offset
        } else {
            self.size
        };

        // Truncate lookups vector
        self.lookups.truncate(truncate_pos);
        self.end_index = self.lookups.last().map_or(self.start_index, |l| l.log_index);
        self.size = new_size;

        self.writer.get_ref().set_len(new_size as u64)?;
        self.writer.seek(SeekFrom::Start(new_size as u64))?;

        Ok(())
    }
}

impl FileOpLogs {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        Self::validate_folder(&path)?;
        let segment_paths = Self::detect_and_sort_existing_segments(&path)?;

        let mut segments = Vec::with_capacity(segment_paths.len());
        for segment_path in segment_paths.iter().take(segment_paths.len().saturating_sub(1)) {
            let segment = Segment::from_path(segment_path)?;
            segments.push(segment);
        }

        let active_segment = Self::take_last_segment_otherwise_init(&path, segment_paths.clone())?;
        Ok(Self { path, active_segment, segments })
    }

    /// Forces any buffered data to be written to disk.
    fn fsync(&mut self) -> Result<()> {
        self.active_segment.writer.flush()?;
        self.active_segment.writer.get_ref().sync_all()?;
        Ok(())
    }

    fn validate_folder(path: &Path) -> Result<(), anyhow::Error> {
        match std::fs::metadata(path) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(anyhow::anyhow!("Path is not a directory"));
                }
            },
            Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(path)?;
            },
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    fn detect_and_sort_existing_segments(path: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        // Pre-allocate with reasonable capacity
        let mut segments_with_indices = Vec::with_capacity(16);

        for entry in std::fs::read_dir(path)?.flatten() {
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();

            if name_str.starts_with("segment_") && name_str.ends_with(".oplog") {
                let number_part = &name_str[8..name_str.len() - 6]; // Remove "segment_" and ".oplog"
                if let Ok(index) = number_part.parse::<u64>() {
                    segments_with_indices.push((index, entry.path()));
                }
            }
        }

        segments_with_indices.sort_unstable_by_key(|(index, _)| *index);
        Ok(segments_with_indices.into_iter().map(|(_, path)| path).collect())
    }

    fn take_last_segment_otherwise_init(
        path: &Path,
        segment_paths: Vec<PathBuf>,
    ) -> Result<Segment, anyhow::Error> {
        if let Some(last_path) = segment_paths.last() {
            Segment::from_path(last_path)
        } else {
            Segment::new(path.join("segment_0.oplog"))
        }
    }

    fn rotate_segment(&mut self) -> Result<()> {
        self.fsync()?;
        let next_index = self.segments.len() + 1;
        let segment_path = self.path.join(format!("segment_{next_index}.oplog"));
        let mut new_active_segment = Segment::new(segment_path)?;

        // Set start_index based on the current active segment's state
        if self.active_segment.lookups.is_empty() {
            new_active_segment.start_index = self.active_segment.start_index;
        } else {
            new_active_segment.start_index = self.active_segment.end_index + 1;
        }
        new_active_segment.end_index = new_active_segment.start_index.saturating_sub(1);

        let old_active_segment = std::mem::replace(&mut self.active_segment, new_active_segment);
        self.segments.push(old_active_segment);
        Ok(())
    }
}

impl TWriteAheadLog for FileOpLogs {
    fn write_many(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        // Pre-calculate total size to optimize buffer allocation
        let total_estimated_size: usize =
            ops.iter().map(|op| serialized_len_with_bincode(WRITE_OP_PREFIX, op)).sum();

        // If all operations fit in current segment, optimize for single write
        if total_estimated_size <= SEGMENT_SIZE.saturating_sub(self.active_segment.size) {
            let mut buffer = Vec::with_capacity(total_estimated_size);
            let mut lookups = Vec::with_capacity(ops.len());

            for op in &ops {
                lookups
                    .push(LookupIndex::new(op.log_index, self.active_segment.size + buffer.len()));
                buffer.push(WRITE_OP_PREFIX as u8);
                let serialized = bincode::encode_to_vec(op, SERDE_CONFIG)?;
                buffer.extend_from_slice(&serialized);
            }

            self.active_segment.writer.write_all(&buffer)?;
            self.active_segment.size += buffer.len();
            self.active_segment.lookups.extend(lookups);
            self.active_segment.end_index = ops.last().unwrap().log_index;
            self.fsync()?;
            return Ok(());
        }

        let mut remaining_ops = &ops[..];
        while !remaining_ops.is_empty() {
            let available_space = SEGMENT_SIZE.saturating_sub(self.active_segment.size);
            let mut chunk_size = 0;
            let mut count = 0;

            // Find how many operations fit in current segment
            for op in remaining_ops {
                let op_size = serialized_len_with_bincode(WRITE_OP_PREFIX, op);
                if chunk_size + op_size > available_space && count > 0 {
                    break;
                }
                chunk_size += op_size;
                count += 1;
            }
            // If nothing fits, rotate and retry
            if count == 0 {
                self.rotate_segment()?;
                continue;
            }

            // Process batch directly with pre-allocated buffer
            let mut buffer = Vec::with_capacity(chunk_size);
            let mut lookups = Vec::with_capacity(count);

            let chunk_to_process = &remaining_ops[..count];
            for op in chunk_to_process {
                lookups
                    .push(LookupIndex::new(op.log_index, self.active_segment.size + buffer.len()));

                buffer.push(WRITE_OP_PREFIX as u8);
                let serialized = bincode::encode_to_vec(op, SERDE_CONFIG)?;
                buffer.extend_from_slice(&serialized);
            }
            self.active_segment.writer.write_all(&buffer)?;
            self.active_segment.size += buffer.len();
            self.active_segment.lookups.extend(lookups);
            self.active_segment.end_index = chunk_to_process.last().unwrap().log_index;

            // just advance the slice. This is an O(1) operation!
            remaining_ops = &remaining_ops[count..];

            // Rotate if more ops remain
            if !remaining_ops.is_empty() {
                self.rotate_segment()?;
            }
        }

        self.fsync()?;
        Ok(())
    }

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        let mut result = Vec::new();
        let all_segments = self.segments.iter().chain(std::iter::once(&self.active_segment));

        for segment in all_segments {
            // Skip segments with no overlap
            if segment.lookups.is_empty()
                || segment.end_index <= start_exclusive
                || segment.start_index > end_inclusive
            {
                continue;
            }

            // Find start position
            let start_pos = segment
                .lookups
                .binary_search_by_key(&(start_exclusive + 1), |lookup| lookup.log_index)
                .unwrap_or_else(|pos| pos);

            // Find end position starting from start_pos for better cache locality
            let end_pos = match segment.lookups[start_pos..]
                .binary_search_by_key(&end_inclusive, |lookup| lookup.log_index)
            {
                Ok(relative_pos) => start_pos + relative_pos + 1,
                Err(relative_pos) => start_pos + relative_pos,
            };

            if start_pos >= end_pos {
                continue;
            }

            // Calculate exact byte range to read
            let start_offset = segment.lookups[start_pos].byte_offset;
            let end_offset = if end_pos < segment.lookups.len() {
                segment.lookups[end_pos].byte_offset
            } else {
                segment.size
            };

            if let Ok(file) = File::open(&segment.path) {
                let mut reader = BufReader::with_capacity(32 * 1024, file); // 32KB read buffer
                if reader.seek(SeekFrom::Start(start_offset as u64)).is_ok() {
                    let read_size = end_offset - start_offset;

                    // Use zeroed buffer - safer and clippy-compliant
                    let mut buffer = vec![0u8; read_size];

                    if reader.read_exact(&mut buffer).is_ok()
                        && let Ok(ops) = LogEntry::deserialize(Bytes::copy_from_slice(&buffer))
                    {
                        result.extend(ops.into_iter().filter(|op| {
                            op.log_index > start_exclusive && op.log_index <= end_inclusive
                        }));
                    }
                }
            }
        }
        result
    }

    /// Replays all existing operations in the op_logs, invoking a callback for each.
    fn replay<F>(&mut self, mut replay_handler: F) -> Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        // Replay all segments in order with streaming to avoid loading everything into memory
        for segment in self.segments.iter_mut().chain(std::iter::once(&mut self.active_segment)) {
            if segment.lookups.is_empty() {
                continue;
            }

            // Stream operations in chunks to reduce memory pressure
            const CHUNK_SIZE: usize = 1000;
            let mut start_idx = 0;

            while start_idx < segment.lookups.len() {
                let end_idx = (start_idx + CHUNK_SIZE).min(segment.lookups.len());
                let start_offset = segment.lookups[start_idx].byte_offset;
                let end_offset = if end_idx < segment.lookups.len() {
                    segment.lookups[end_idx].byte_offset
                } else {
                    segment.size
                };

                if let Ok(file) = File::open(&segment.path) {
                    let mut reader = BufReader::with_capacity(64 * 1024, file);
                    if reader.seek(SeekFrom::Start(start_offset as u64)).is_ok() {
                        let read_size = end_offset - start_offset;
                        let mut buffer = vec![0u8; read_size];

                        if reader.read_exact(&mut buffer).is_ok()
                            && let Ok(operations) =
                                LogEntry::deserialize(Bytes::copy_from_slice(&buffer))
                        {
                            operations.into_iter().for_each(&mut replay_handler);
                        }
                    }
                }
                start_idx = end_idx;
            }
        }

        Ok(())
    }

    fn read_at(&mut self, log_index: u64) -> Option<WriteOperation> {
        // Check active segment first (most likely to contain recent reads)
        if self.active_segment.start_index <= log_index
            && self.active_segment.end_index >= log_index
            && let Some(offset) = self.active_segment.find_offset(log_index)
        {
            return self.active_segment.read_at_offset(offset).ok();
        }

        // Then check sealed segments in reverse order (more recent first)
        for segment in self.segments.iter_mut().rev() {
            if segment.start_index <= log_index
                && segment.end_index >= log_index
                && let Some(offset) = segment.find_offset(log_index)
            {
                return segment.read_at_offset(offset).ok();
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.segments.is_empty() && self.active_segment.lookups.is_empty()
    }

    fn truncate_after(&mut self, log_index: u64) {
        // Early return if no truncation needed
        if self.segments.is_empty() && self.active_segment.end_index <= log_index {
            return;
        }

        // Find segments to remove in reverse order for efficient removal
        let mut segments_to_remove = Vec::new();
        for (i, segment) in self.segments.iter().enumerate().rev() {
            if segment.start_index > log_index {
                segments_to_remove.push(i);
            } else {
                break;
            }
        }

        // Remove segments and their files
        for &i in &segments_to_remove {
            let segment = self.segments.remove(i);
            let _ = std::fs::remove_file(&segment.path);
        }

        // Check if the last sealed segment needs to be truncated and promoted
        if let Some(last_segment) = self.segments.last()
            && last_segment.end_index > log_index
            && let Some(mut new_active) = self.segments.pop()
        {
            if new_active.truncate(log_index).is_ok() {
                let _ = std::fs::remove_file(&self.active_segment.path);
                self.active_segment = new_active;
            }
            return;
        }

        if self.active_segment.end_index <= log_index {
            return;
        }

        // Handle active segment truncation
        if self.active_segment.start_index > log_index {
            let _ = std::fs::remove_file(&self.active_segment.path);
            let new_segment_idx = self.segments.len();
            let new_path = self.path.join(format!("segment_{}.oplog", new_segment_idx));
            let mut new_active = Segment::new(new_path).expect("Failed to create new segment");
            new_active.start_index = self.segments.last().map_or(0, |s| s.end_index + 1);
            self.active_segment = new_active;
        } else {
            let _ = self.active_segment.truncate(log_index);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domains::{caches::cache_objects::CacheEntry, replications::LogEntry};

    use super::*;
    use anyhow::Result;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn set_helper(index: u64, term: u64) -> WriteOperation {
        WriteOperation {
            entry: LogEntry::Set { entry: CacheEntry::new("foo".to_string(), "bar") },
            log_index: index,
            term,
            session_req: None,
        }
    }

    #[test]
    fn test_new_creates_oplogs() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        assert!(!path.exists());
        assert!(FileOpLogs::new(&path).is_ok());

        // THEN
        assert!(path.exists());

        Ok(())
    }

    #[test]
    fn test_append_stores_to_disk() {
        // GIVEN
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path).unwrap();
        let request = LogEntry::Set { entry: CacheEntry::new("foo".to_string(), "bar") };

        let write_op =
            WriteOperation { entry: request.clone(), log_index: 0, term: 0, session_req: None };

        // WHEN
        op_logs.write_many(vec![write_op]).unwrap();
        drop(op_logs);

        // THEN
        let mut file = std::fs::File::open(path.join("segment_0.oplog")).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let (encoded, _): (WriteOperation, usize) =
            bincode::decode_from_slice(&buf[1..], SERDE_CONFIG).unwrap();

        assert_eq!(encoded.entry, request);
    }

    #[test]
    fn test_replay_multiple_operations() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        let mut op_logs = FileOpLogs::new(&path)?;
        op_logs.write_many(vec![set_helper(0, 0)])?;
        op_logs.write_many(vec![set_helper(1, 0)])?;
        op_logs.write_many(vec![set_helper(2, 1)])?;

        let mut op_logs = FileOpLogs::new(&path)?;
        let mut ops = Vec::new();

        op_logs.replay(|op| {
            ops.push(op);
        })?;

        // THEN
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0], set_helper(0, 0));
        assert_eq!(ops[1], set_helper(1, 0));
        assert_eq!(ops[2], set_helper(2, 1));

        Ok(())
    }

    #[test]
    #[ignore = "This is desired behavior. However, currently deserialize fails if any part of the file is corrupted."]
    fn test_replay_partial_data() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        // Append three ops.
        {
            let mut op_logs = FileOpLogs::new(&path)?;
            op_logs.write_many(vec![set_helper(0, 0)])?;
            op_logs.write_many(vec![set_helper(1, 0)])?;
            op_logs.write_many(vec![set_helper(2, 1)])?;
        }

        // Corrupt file content by truncating to the first half.
        // We should only have one complete op.
        {
            let mut file = OpenOptions::new().read(true).open(&path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;

            data.truncate(data.len() / 2);

            let mut file = OpenOptions::new().write(true).truncate(true).open(&path)?;
            file.write_all(&data)?;
        }

        let mut op_logs = FileOpLogs::new(&path)?;
        let mut ops = Vec::new();

        assert!(
            op_logs
                .replay(|op| {
                    ops.push(op);
                })
                .is_err()
        );

        // THEN
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0], set_helper(0, 0));

        Ok(())
    }

    #[test]
    fn test_new_creates_initial_segment() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // WHEN
        let op_logs = FileOpLogs::new(path)?;

        // THEN

        let segment = op_logs.active_segment;
        assert_eq!(segment.start_index, 0);
        assert_eq!(segment.end_index, 0);
        assert_eq!(segment.size, 0);
        assert!(segment.path.exists());
        assert!(segment.path.ends_with("segment_0.oplog"));

        Ok(())
    }

    #[test]
    fn test_new_loads_existing_segment_metadata() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create initial segment with some operations
        {
            let mut op_logs = FileOpLogs::new(path)?;
            op_logs.write_many(vec![set_helper(10, 1)])?;
            op_logs.write_many(vec![set_helper(11, 1)])?;
        }

        // WHEN
        let op_logs = FileOpLogs::new(path)?;

        // THEN

        let segment = op_logs.active_segment;
        assert_eq!(segment.start_index, 10);
        assert_eq!(segment.end_index, 11);
        assert!(segment.size > 0);
        assert!(segment.path.exists());
        assert!(segment.path.ends_with("segment_0.oplog"));

        // Verify we can read the operations
        let file = OpenOptions::new().read(true).open(&segment.path)?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let bytes = Bytes::copy_from_slice(&buf[..]);
        let operations = LogEntry::deserialize(bytes)?;
        assert_eq!(operations.len(), 2);

        Ok(())
    }

    #[test]
    fn test_new_handles_multiple_segments() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create multiple segments by forcing rotation

        let mut op_logs = FileOpLogs::new(path)?;
        // Fill first segment
        for i in 0..100 {
            op_logs.write_many(vec![WriteOperation {
                entry: LogEntry::Set {
                    entry: CacheEntry::new(format!("key_{i}"), format!("value_{i}").as_str()),
                },
                log_index: i as u64,
                term: 1,
                session_req: None,
            }])?;
        }
        // Force rotation
        op_logs.rotate_segment()?;
        // Add to new segment
        op_logs.write_many(vec![WriteOperation {
            entry: LogEntry::Set { entry: CacheEntry::new("new".to_string(), "value") },
            log_index: 100,
            term: 1,
            session_req: None,
        }])?;

        // WHEN
        let mut op_logs = FileOpLogs::new(path)?;

        // THEN

        assert_eq!(op_logs.active_segment.start_index, 100);
        assert_eq!(op_logs.active_segment.end_index, 100);
        assert!(op_logs.active_segment.size > 0);
        assert!(op_logs.active_segment.path.exists());
        assert!(op_logs.active_segment.path.ends_with("segment_1.oplog"));

        // Verify previous segment exists
        let prev_segment_path = path.join("segment_0.oplog");
        assert!(prev_segment_path.exists());

        // Verify we can read operations from both segments
        let mut ops = Vec::new();
        op_logs.replay(|op| ops.push(op))?;
        assert_eq!(ops.len(), 101);

        Ok(())
    }

    #[test]
    fn test_new_handles_corrupted_segment() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create a segment and corrupt it
        let mut op_logs = FileOpLogs::new(path)?;
        op_logs.write_many(vec![set_helper(0, 1)])?;

        // Corrupt the segment file
        let segment_path = path.join("segment_0.oplog");
        let mut file = OpenOptions::new().write(true).open(&segment_path)?;
        file.write_all(b"corrupted data")?;

        // WHEN/THEN
        assert!(FileOpLogs::new(path).is_err());

        Ok(())
    }

    #[test]
    fn test_range_empty_log() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path();
        let op_logs = FileOpLogs::new(path)?;

        let range_result = op_logs.range(0, 10);
        assert!(range_result.is_empty());

        let range_result = op_logs.range(10, 20);
        assert!(range_result.is_empty());
        Ok(())
    }

    // Helper to create dummy operations
    fn create_ops(start_index: u64, count: usize, term: u64) -> Vec<WriteOperation> {
        (0..count)
            .map(|i| WriteOperation {
                entry: LogEntry::Set {
                    entry: CacheEntry::new(
                        format!("key_{}", start_index + i as u64),
                        format!("value_{}", start_index + i as u64).as_str(),
                    ),
                },
                log_index: start_index + i as u64,
                term,
                session_req: None,
            })
            .collect()
    }

    #[test]
    fn test_range_single_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path).unwrap();

        let ops_to_append = create_ops(0, 6, 1); // Indices 0, 1, 2, 3, 4, 5
        op_logs.write_many(ops_to_append.clone()).unwrap();

        // Range fully within the segment (1 < i <= 3) -> 2, 3
        let result = op_logs.range(1, 3);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].log_index, 2);
        assert_eq!(result[1].log_index, 3);

        // Range covering start (0 < i <= 2) -> 1, 2
        let result = op_logs.range(0, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].log_index, 1);
        assert_eq!(result[1].log_index, 2);

        // Range covering end (3 < i <= 5) -> 4, 5
        let result = op_logs.range(3, 5);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].log_index, 4);
        assert_eq!(result[1].log_index, 5);

        // Range covering entire segment (u64::MIN < i <= 5) -> 1, 2, 3, 4, 5 (if log starts at 0)
        // If log starts at 0, u64::MIN is 0, so we need i > 0.
        let result = op_logs.range(u64::MIN, 5); // Should get 1, 2, 3, 4, 5
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].log_index, 1);
        assert_eq!(result[4].log_index, 5);

        // Range entirely before (10 < i <= 15) -> empty
        let result = op_logs.range(10, 15);
        assert!(result.is_empty());

        // Range entirely after (5 < i <= 10) -> empty (since max index is 5)
        let result = op_logs.range(5, 10);
        assert!(result.is_empty());

        // Range with start_exclusive equal to end_inclusive (2 < i <= 2) -> empty
        let result = op_logs.range(2, 2);
        assert!(result.is_empty());

        // Range with start_exclusive + 1 == end_inclusive (2 < i <= 3) -> 3
        let result = op_logs.range(2, 3);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].log_index, 3);
    }

    #[test]
    fn test_range_multiple_segments() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut op_logs2 = FileOpLogs::new(path).unwrap();

        let ops_seg1 = create_ops(0, 10, 1); // Ops 0-9
        op_logs2.write_many(ops_seg1.clone()).unwrap();
        op_logs2.rotate_segment().unwrap(); // segment_0 (0-9) sealed

        let ops_seg2 = create_ops(10, 10, 2); // Ops 10-19
        op_logs2.write_many(ops_seg2.clone()).unwrap();
        op_logs2.rotate_segment().unwrap(); // segment_1 (10-19) sealed

        let ops_active = create_ops(20, 5, 3); // Ops 20-24
        op_logs2.write_many(ops_active.clone()).unwrap(); // segment_2 (20-24) active

        println!("Segments for multi-segment test: {:?}", op_logs2.segments);
        println!("Active segment for multi-segment test: {:?}", op_logs2.active_segment);

        // Range across sealed segments (5 < i <= 15) -> 6..=15
        let result = op_logs2.range(5, 15);
        assert_eq!(result.len(), 10);
        assert_eq!(result[0].log_index, 6);
        assert_eq!(result[9].log_index, 15);

        // Range across sealed and active segments (18 < i <= 22) -> 19..=22
        let result = op_logs2.range(18, 22);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].log_index, 19);
        assert_eq!(result[3].log_index, 22);

        // Range covering last ops in sealed and first ops in active (9 < i <= 21) -> 10..=21
        let result = op_logs2.range(9, 21);
        assert_eq!(result.len(), 12);
        assert_eq!(result[0].log_index, 10);
        assert_eq!(result[11].log_index, 21);

        // Range covering everything (u64::MIN < i <= 24) -> 1..=24 (if log starts at 0)
        let result = op_logs2.range(u64::MIN, 24);
        assert_eq!(result.len(), 24); // Indices 1 through 24 (24 ops)
        assert_eq!(result[0].log_index, 1);
        assert_eq!(result[23].log_index, 24);

        // Range starting mid-segment (12 < i <= 20) -> 13..=20
        let result = op_logs2.range(12, 20);
        assert_eq!(result.len(), 8);
        assert_eq!(result[0].log_index, 13);
        assert_eq!(result[7].log_index, 20);

        // Range up to the first element (u64::MIN < i <= 0) -> empty (since i must be > 0)
        let result = op_logs2.range(u64::MIN, 0);
        assert!(result.is_empty());
    }

    // --- Tests for read_at ---

    #[test]
    fn test_read_at_empty_log() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path).unwrap();

        let op = op_logs.read_at(0);
        assert!(op.is_none());

        let op = op_logs.read_at(100);
        assert!(op.is_none());
    }

    #[test]
    fn test_read_at_single_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path).unwrap();

        let ops_to_append = create_ops(0, 5, 1); // Indices 0-4
        op_logs.write_many(ops_to_append.clone()).unwrap();

        // Read existing ops
        let op0 = op_logs.read_at(0);
        assert_eq!(op0, Some(ops_to_append[0].clone()));

        let op3 = op_logs.read_at(3);
        assert_eq!(op3, Some(ops_to_append[3].clone()));

        let op4 = op_logs.read_at(4);
        assert_eq!(op4, Some(ops_to_append[4].clone()));

        // Read non-existing ops
        let op5 = op_logs.read_at(5);
        assert!(op5.is_none());

        let op100 = op_logs.read_at(100);
        assert!(op100.is_none());
    }

    #[test]
    fn test_read_at_multiple_segments() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path).unwrap();

        let ops_seg1 = create_ops(0, 10, 1); // Ops 0-9
        op_logs.write_many(ops_seg1.clone()).unwrap();
        op_logs.rotate_segment().unwrap(); // segment_0 (0-9) sealed

        let ops_seg2 = create_ops(10, 10, 2); // Ops 10-19
        op_logs.write_many(ops_seg2.clone()).unwrap();
        op_logs.rotate_segment().unwrap(); // segment_1 (10-19) sealed

        let ops_active = create_ops(20, 5, 3); // Ops 20-24
        op_logs.write_many(ops_active.clone()).unwrap(); // segment_2 (20-24) active

        // Read from first sealed segment
        let op_s1_5 = op_logs.read_at(5);
        assert_eq!(op_s1_5, Some(ops_seg1[5].clone()));

        // Read from second sealed segment
        let op_s2_15 = op_logs.read_at(15);
        assert_eq!(op_s2_15, Some(ops_seg2[5].clone())); // Index 15 is the 6th op in ops_seg2 (index 5)

        // Read from active segment
        let op_active_22 = op_logs.read_at(22);
        assert_eq!(op_active_22, Some(ops_active[2].clone())); // Index 22 is the 3rd op in ops_active (index 2)

        // Read non-existing ops
        let op_nonexistent = op_logs.read_at(100);
        assert!(op_nonexistent.is_none());

        let op_between = op_logs.read_at(9); // Should find index 9 (last in seg1)
        assert_eq!(op_between, Some(ops_seg1[9].clone()));
        let op_between_start = op_logs.read_at(10); // Should find index 10 (first in seg2)
        assert_eq!(op_between_start, Some(ops_seg2[0].clone()));
    }

    #[test]
    fn test_index_data_accuracy() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path)?;

        // Write some operations
        let ops = vec![set_helper(1, 1), set_helper(2, 1)];

        // Append operations
        op_logs.write_many(ops)?;

        // Verify index data is correctly maintained
        assert_eq!(op_logs.active_segment.lookups.len(), 2);
        assert_eq!(op_logs.active_segment.lookups[0], LookupIndex::new(1, 0));
        assert!(op_logs.active_segment.lookups[1].log_index == 2);
        assert!(op_logs.active_segment.lookups[1].byte_offset > 0);

        // Verify we can read operations using the index
        let read_op1 = op_logs.read_at(1);
        let read_op2 = op_logs.read_at(2);

        assert!(read_op1.is_some());
        assert!(read_op2.is_some());
        assert_eq!(read_op1.unwrap().log_index, 1);
        assert_eq!(read_op2.unwrap().log_index, 2);

        Ok(())
    }

    #[test]
    fn test_index_data_after_rotation() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path)?;

        // Fill first segment

        op_logs.write_many(
            (0..100)
                .map(|i| WriteOperation {
                    entry: LogEntry::Set {
                        entry: CacheEntry::new(format!("key_{i}"), format!("value_{i}").as_str()),
                    },
                    log_index: i as u64,
                    term: 1,
                    session_req: None,
                })
                .collect(),
        )?;

        // Force rotation
        op_logs.rotate_segment()?;

        // Verify index data in sealed segment
        let sealed_segment = &op_logs.segments[0];
        assert_eq!(sealed_segment.lookups.len(), 100);
        assert_eq!(sealed_segment.lookups[0], LookupIndex::new(0, 0));
        assert!(sealed_segment.lookups[99].log_index == 99);

        // Add to new segment
        op_logs.write_many(vec![WriteOperation {
            entry: LogEntry::Set { entry: CacheEntry::new("new".to_string(), "value") },
            log_index: 100,
            term: 1,
            session_req: None,
        }])?;

        // Verify index data in active segment
        assert_eq!(op_logs.active_segment.lookups.len(), 1);
        assert_eq!(op_logs.active_segment.lookups[0], LookupIndex::new(100, 0));

        Ok(())
    }

    #[test]
    fn test_index_data_recovery() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create initial log with some operations

        let mut op_logs = FileOpLogs::new(path)?;
        op_logs.write_many(
            (0..50)
                .map(|i| WriteOperation {
                    entry: LogEntry::Set {
                        entry: CacheEntry::new(format!("key_{i}"), format!("value_{i}").as_str()),
                    },
                    log_index: i as u64,
                    term: 1,
                    session_req: None,
                })
                .collect(),
        )?;

        // Reopen the log
        let mut op_logs = FileOpLogs::new(path)?;

        // Verify index data was recovered correctly
        assert_eq!(op_logs.active_segment.lookups.len(), 50);
        for i in 0..50 {
            assert_eq!(op_logs.active_segment.lookups[i].log_index, i as u64);
        }

        // Verify we can read operations using the recovered index
        for i in 0..50 {
            let op = op_logs.read_at(i as u64);
            assert!(op.is_some());
            assert_eq!(op.unwrap().log_index, i as u64);
        }

        Ok(())
    }

    #[test]
    fn test_truncate_after() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path();
        let mut op_logs = FileOpLogs::new(path)?;

        let ops = create_ops(0, 20, 1); // ops 0-19
        op_logs.write_many(ops)?;
        op_logs.rotate_segment()?;
        let ops2 = create_ops(20, 20, 1); // ops 20-39
        op_logs.write_many(ops2)?;

        // Truncate in the middle of the active segment
        op_logs.truncate_after(25);
        assert_eq!(op_logs.read_at(25).unwrap().log_index, 25);
        assert!(op_logs.read_at(26).is_none());
        assert_eq!(op_logs.active_segment.end_index, 25);

        // Truncate in the sealed segment
        op_logs.truncate_after(10);
        assert_eq!(op_logs.read_at(10).unwrap().log_index, 10);
        assert!(op_logs.read_at(11).is_none());
        assert!(op_logs.read_at(20).is_none());
        assert_eq!(op_logs.active_segment.end_index, 10);
        assert_eq!(op_logs.segments.len(), 0);

        Ok(())
    }
}
