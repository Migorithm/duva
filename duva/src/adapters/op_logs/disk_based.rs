use crate::domains::QueryIO;
use crate::domains::operation_logs::LogEntry;
use crate::domains::operation_logs::WriteOperation;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::query_io::{SERDE_CONFIG, WRITE_OP_PREFIX};
use anyhow::Result;

use bytes::Bytes;

use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Write;
use std::io::{BufReader, Seek, SeekFrom};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

const SEGMENT_SIZE: usize = 1024 * 1024; // 1MB per segment

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
        let file =
            OpenOptions::new().create(true).read(true).write(true).append(true).open(&path)?;

        let initial_size = file.metadata()?.len() as usize;

        let writer = BufWriter::new(file);
        Ok(Self {
            path,
            writer,
            start_index: 0,
            end_index: 0,
            size: initial_size,
            lookups: Vec::new(),
        })
    }

    fn read_operations(&mut self) -> Result<Vec<WriteOperation>> {
        let mut file = File::open(&self.path)?; // Open a temporary read handle
        file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        LogEntry::deserialize(Bytes::copy_from_slice(&buf[..]))
    }

    // Add method to read operation at specific offset
    fn read_at_offset(&mut self, offset: usize) -> Result<WriteOperation> {
        // Open a temporary, read-only file handle.
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        // Seek to the exact starting byte of the operation.
        reader.seek(SeekFrom::Start(offset as u64))?;

        // Decode only ONE operation from the stream.
        // This is vastly more efficient than read_to_end.
        let mut prefix_buf = [0u8; 1];
        reader.read_exact(&mut prefix_buf)?;

        // 2. Check if it's the `REPLICATE_PREFIX`.
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
        let writer = BufWriter::new(file);

        // Open a separate read-only handle to read the contents.
        let mut reader = File::open(path)?;
        let mut buf = Vec::with_capacity(file_size);
        reader.read_to_end(&mut buf)?;

        // Use your custom deserializer that understands the on-disk format.
        let operations = if buf.is_empty() {
            Vec::new()
        } else {
            LogEntry::deserialize(Bytes::copy_from_slice(&buf))?
        };

        let (start_index, end_index) = match (operations.first(), operations.last()) {
            | (Some(first), Some(last)) => (first.log_index, last.log_index),
            | _ => (0, 0),
        };
        let mut current_offset = 0;
        let mut lookups = Vec::with_capacity(operations.len());
        for op in operations.into_iter() {
            lookups.push(LookupIndex::new(op.log_index, current_offset));
            let encoded_size =
                bincode::encode_to_vec(&op, SERDE_CONFIG).map(|v| v.len()).unwrap_or(0); // Handle encoding error gracefully
            current_offset += WRITE_OP_PREFIX.len_utf8() + encoded_size
        }

        Ok(Segment { path: path.clone(), start_index, end_index, size: file_size, lookups, writer })
    }

    fn find_offset(&self, log_index: u64) -> Option<usize> {
        self.lookups
            .binary_search_by_key(&log_index, |index| index.log_index)
            .ok()
            .map(|found_index| self.lookups[found_index].byte_offset)
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
            | Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(anyhow::anyhow!("Path is not a directory"));
                }
            },
            | Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(path)?;
            },
            | Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    fn detect_and_sort_existing_segments(path: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let re = Regex::new(r"^segment_(\d+)\.oplog$")?;
        let mut segments_with_indices = Vec::new();
        for entry in std::fs::read_dir(path)? {
            if let Ok(entry) = entry
                && let Some(captures) = re.captures(entry.file_name().to_string_lossy().as_ref())
                && let Ok(index) = captures[1].parse::<u64>()
            {
                segments_with_indices.push((index, entry.path()));
            }
        }
        segments_with_indices.sort_by_key(|(index, _)| *index);
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
        new_active_segment.start_index = self.active_segment.end_index + 1;
        new_active_segment.end_index = self.active_segment.end_index;
        let old_active_segment = std::mem::replace(&mut self.active_segment, new_active_segment);
        self.segments.push(old_active_segment);
        Ok(())
    }

    fn read_ops_from_reader(
        &self,
        reader: &mut BufReader<File>,
        start_exclusive: u64,
        end_inclusive: u64,
    ) -> Result<Vec<WriteOperation>> {
        let mut collected_ops = Vec::new();
        let mut buffer = Vec::new();

        reader.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok(collected_ops);
        }

        let bytes = Bytes::copy_from_slice(&buffer);

        let operations_in_buffer = LogEntry::deserialize(bytes)?;

        for op in operations_in_buffer {
            if op.log_index > end_inclusive {
                // Reached beyond the end of the desired range
                break;
            }
            if op.log_index > start_exclusive {
                // Operation is within the desired range
                collected_ops.push(op);
            }
        }

        Ok(collected_ops)
    }
}

impl TWriteAheadLog for FileOpLogs {
    /// Appends a single `WriteOperation` to the file.
    fn append(&mut self, op: WriteOperation) -> Result<()> {
        if self.active_segment.size >= SEGMENT_SIZE {
            self.rotate_segment()?;
        }
        let log_index = op.log_index;
        let serialized = QueryIO::WriteOperation(op).serialize();

        // No need to seek, as file is append mode on
        self.active_segment.writer.write_all(&serialized)?;
        self.fsync()?; // Sync after write

        self.active_segment.lookups.push(LookupIndex::new(log_index, self.active_segment.size));
        self.active_segment.size += serialized.len();
        self.active_segment.end_index = log_index;
        Ok(())
    }

    fn append_many(&mut self, mut ops: Vec<WriteOperation>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        // Use a mutable slice to process the ops in chunks
        let mut ops_slice = &mut ops[..];

        while !ops_slice.is_empty() {
            let space_available = SEGMENT_SIZE.saturating_sub(self.active_segment.size);
            let mut current_chunk_bytes = Vec::new();
            let mut new_lookups = Vec::new();
            let mut ops_in_chunk = 0;
            let mut last_log_index = self.active_segment.end_index;

            // Determine how many operations from the remaining slice fit into the active segment
            for op in ops_slice.iter() {
                let serialized = QueryIO::WriteOperation(op.clone()).serialize(); // Clone is needed here
                if current_chunk_bytes.len() + serialized.len() > space_available {
                    break;
                }

                new_lookups.push(LookupIndex::new(
                    op.log_index,
                    self.active_segment.size + current_chunk_bytes.len(),
                ));
                current_chunk_bytes.extend_from_slice(&serialized);
                last_log_index = op.log_index;
                ops_in_chunk += 1;
            }

            // If we can't even fit the first operation, rotate the segment and retry
            if ops_in_chunk == 0 && !ops_slice.is_empty() {
                self.rotate_segment()?;
                // `continue` will re-evaluate the loop with the new empty active_segment
                continue;
            }

            // Write the entire chunk of operations to the file in one go
            if !current_chunk_bytes.is_empty() {
                self.active_segment.writer.write_all(&current_chunk_bytes)?;

                // Update segment metadata
                self.active_segment.size += current_chunk_bytes.len();
                self.active_segment.lookups.append(&mut new_lookups);
                self.active_segment.end_index = last_log_index;
            }

            // Move the slice forward and rotate the segment for the next chunk
            ops_slice = &mut ops_slice[ops_in_chunk..];
            if !ops_slice.is_empty() {
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
            // if no overlaps, continue
            if !(segment.end_index > start_exclusive && segment.start_index <= end_inclusive) {
                continue;
            }

            let first_included_index = start_exclusive.saturating_add(1); // Avoid overflow
            let starting_idx_in_index = segment
                .lookups
                .binary_search_by(|index| index.log_index.cmp(&first_included_index))
                .unwrap_or_else(|pos| pos); // If not found, pos is where it would be inserted

            if starting_idx_in_index < segment.lookups.len() {
                let start_byte_offset = segment.lookups[starting_idx_in_index].byte_offset;

                if let Ok(file) = OpenOptions::new().read(true).open(&segment.path) {
                    let mut reader = BufReader::new(file);
                    if reader.seek(std::io::SeekFrom::Start(start_byte_offset as u64)).is_ok()
                        && let Ok(ops) =
                            self.read_ops_from_reader(&mut reader, start_exclusive, end_inclusive)
                    {
                        result.extend(ops);
                    }
                }
            } else if first_included_index <= segment.start_index {
                if segment.start_index <= end_inclusive
                    && let Ok(file) = OpenOptions::new().read(true).open(&segment.path)
                {
                    let mut reader = BufReader::new(file); // Starts at offset 0
                    if let Ok(ops) =
                        self.read_ops_from_reader(&mut reader, start_exclusive, end_inclusive)
                    {
                        result.extend(ops);
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
        // Replay all segments in order
        for segment in self.segments.iter_mut().chain(std::iter::once(&mut self.active_segment)) {
            let operations: Vec<WriteOperation> = segment.read_operations()?;
            operations.into_iter().for_each(&mut replay_handler);
        }

        Ok(())
    }

    fn read_at(&mut self, log_index: u64) -> Option<WriteOperation> {
        for segment in self.segments.iter_mut().chain(std::iter::once(&mut self.active_segment)) {
            // if overlab is found
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
        // Remove segments after the truncation point
        self.segments.retain(|segment| segment.end_index <= log_index);

        // If active segment needs truncation

        if self.active_segment.start_index <= log_index && self.active_segment.end_index > log_index
        {
            // TODO: Implement truncation of active segment
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::domains::operation_logs::LogEntry;

    use super::*;
    use anyhow::Result;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn set_helper(index: u64, term: u64) -> WriteOperation {
        WriteOperation {
            entry: LogEntry::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
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
        let request = LogEntry::Set { key: "foo".into(), value: "bar".into(), expires_at: None };
        let write_op =
            WriteOperation { entry: request.clone(), log_index: 0, term: 0, session_req: None };

        // WHEN
        op_logs.append(write_op).unwrap();
        drop(op_logs);

        // THEN
        let mut file = std::fs::File::open(path.join("segment_0.oplog")).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let (encoded, _): (WriteOperation, usize) =
            bincode::decode_from_slice(&buf[1..], bincode::config::standard()).unwrap();

        assert_eq!(encoded.entry, request);
    }

    #[test]
    fn test_replay_multiple_operations() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        let mut op_logs = FileOpLogs::new(&path)?;
        op_logs.append(set_helper(0, 0))?;
        op_logs.append(set_helper(1, 0))?;
        op_logs.append(set_helper(2, 1))?;

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
            op_logs.append(set_helper(0, 0))?;
            op_logs.append(set_helper(1, 0))?;
            op_logs.append(set_helper(2, 1))?;
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
            op_logs.append(set_helper(10, 1))?;
            op_logs.append(set_helper(11, 1))?;
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
            op_logs.append(WriteOperation {
                entry: LogEntry::Set {
                    key: format!("key_{i}"),
                    value: format!("value_{i}"),
                    expires_at: None,
                },
                log_index: i as u64,
                term: 1,
                session_req: None,
            })?;
        }
        // Force rotation
        op_logs.rotate_segment()?;
        // Add to new segment
        op_logs.append(WriteOperation {
            entry: LogEntry::Set { key: "new".into(), value: "value".into(), expires_at: None },
            log_index: 100,
            term: 1,
            session_req: None,
        })?;

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
        op_logs.append(set_helper(0, 1))?;

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
                    key: format!("key_{}", start_index + i as u64),
                    value: format!("value_{}", start_index + i as u64),
                    expires_at: None,
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
        op_logs.append_many(ops_to_append.clone()).unwrap();

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
        op_logs2.append_many(ops_seg1.clone()).unwrap();
        op_logs2.rotate_segment().unwrap(); // segment_0 (0-9) sealed

        let ops_seg2 = create_ops(10, 10, 2); // Ops 10-19
        op_logs2.append_many(ops_seg2.clone()).unwrap();
        op_logs2.rotate_segment().unwrap(); // segment_1 (10-19) sealed

        let ops_active = create_ops(20, 5, 3); // Ops 20-24
        op_logs2.append_many(ops_active.clone()).unwrap(); // segment_2 (20-24) active

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
        op_logs.append_many(ops_to_append.clone()).unwrap();

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
        op_logs.append_many(ops_seg1.clone()).unwrap();
        op_logs.rotate_segment().unwrap(); // segment_0 (0-9) sealed

        let ops_seg2 = create_ops(10, 10, 2); // Ops 10-19
        op_logs.append_many(ops_seg2.clone()).unwrap();
        op_logs.rotate_segment().unwrap(); // segment_1 (10-19) sealed

        let ops_active = create_ops(20, 5, 3); // Ops 20-24
        op_logs.append_many(ops_active.clone()).unwrap(); // segment_2 (20-24) active

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
        for op in ops.clone() {
            op_logs.append(op)?;
        }

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
        for i in 0..100 {
            op_logs.append(WriteOperation {
                entry: LogEntry::Set {
                    key: format!("key_{i}"),
                    value: format!("value_{i}"),
                    expires_at: None,
                },
                log_index: i as u64,
                term: 1,
                session_req: None,
            })?;
        }

        // Force rotation
        op_logs.rotate_segment()?;

        // Verify index data in sealed segment
        let sealed_segment = &op_logs.segments[0];
        assert_eq!(sealed_segment.lookups.len(), 100);
        assert_eq!(sealed_segment.lookups[0], LookupIndex::new(0, 0));
        assert!(sealed_segment.lookups[99].log_index == 99);

        // Add to new segment
        op_logs.append(WriteOperation {
            entry: LogEntry::Set { key: "new".into(), value: "value".into(), expires_at: None },
            log_index: 100,
            term: 1,
            session_req: None,
        })?;

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
        {
            let mut op_logs = FileOpLogs::new(path)?;
            for i in 0..50 {
                op_logs.append(WriteOperation {
                    entry: LogEntry::Set {
                        key: format!("key_{i}"),
                        value: format!("value_{i}"),
                        expires_at: None,
                    },
                    log_index: i as u64,
                    term: 1,
                    session_req: None,
                })?;
            }
        }

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
}
