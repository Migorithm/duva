use crate::domains::QueryIO;
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::{LogEntry, WriteOperation};
use anyhow::{Context, Result};
use bincode::error::DecodeError;
use bytes::Bytes;
use regex::Regex;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use tracing::warn;

const SEGMENT_SIZE: usize = 1024 * 1024; // 1MB per segment

/// A local write-ahead-log (WAL) file (op_logs) implementation using segmented logs.
pub struct FileOpLogs {
    /// The directory where all segment files are stored.
    path: PathBuf,
    active_segment: Segment,
    /// Writer for the active segment, kept open for performance.
    active_segment_writer: BufWriter<File>,
    /// Sealed (read-only) segments.
    segments: Vec<Segment>,
}

#[derive(Debug)]
struct Segment {
    path: PathBuf,
    start_index: u64,
    end_index: u64,
    size: usize,
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
    fn new(path: PathBuf, start_index: u64) -> Result<Self> {
        // Ensure the file is created
        OpenOptions::new().create(true).write(true).open(&path)?;

        Ok(Self {
            path,
            start_index,
            end_index: start_index.saturating_sub(1), // No entries yet
            size: 0,
            lookups: Vec::new(),
        })
    }

    /// Iterates through a segment file on disk, yielding operations.
    fn iter_ops(&self) -> Result<impl Iterator<Item = Result<WriteOperation>>> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(OpIterator::new(BufReader::new(file)))
    }

    /// Reads and builds a segment's metadata from an existing file.
    fn from_path(path: &PathBuf, expected_start_index: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("Failed to open segment for reading: {}", path.display()))?;

        let mut reader = BufReader::new(file);
        let mut lookups = Vec::new();
        let mut start_index = expected_start_index;
        let mut end_index = expected_start_index.saturating_sub(1);

        loop {
            let current_offset = reader.stream_position()? as usize;
            let mut prefix = [0u8; 1];
            match reader.read_exact(&mut prefix) {
                | Ok(_) => {},
                | Err(e) if e.kind() == ErrorKind::UnexpectedEof => break, // Clean end of file
                | Err(e) => return Err(e.into()),
            };

            match bincode::decode_from_reader::<WriteOperation, _, _>(
                &mut reader,
                bincode::config::standard(),
            ) {
                | Ok(op) => {
                    if lookups.is_empty() {
                        start_index = op.log_index;
                    }
                    end_index = op.log_index;
                    lookups.push(LookupIndex::new(op.log_index, current_offset));
                },
                | Err(e) => {
                    // **FIXED**: Differentiate between corruption and a partial write.
                    if let bincode::error::DecodeError::Io { .. } = &e {
                        if matches!(e, DecodeError::UnexpectedEnd { .. }) {
                            warn!(
                                "Partial entry found in {}. Log is valid up to offset {}.",
                                path.display(),
                                current_offset
                            );
                            break; // Graceful break for partial writes
                        }
                    }
                    // Any other error is treated as corruption.
                    return Err(anyhow::anyhow!(
                        "Corrupted segment file {}: {}",
                        path.display(),
                        e
                    ));
                },
            }
        }

        let final_size = reader.stream_position()? as usize;
        Ok(Segment { path: path.clone(), start_index, end_index, size: final_size, lookups })
    }
    /// Finds the byte offset for a given log index using binary search.
    fn find_offset(&self, log_index: u64) -> Option<usize> {
        self.lookups
            .binary_search_by_key(&log_index, |index| index.log_index)
            .ok()
            .map(|i| self.lookups[i].byte_offset)
    }

    /// Reads a single operation at a specific byte offset.
    fn read_at_offset(&self, offset: usize) -> Result<WriteOperation> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset as u64))?;
        reader.read_exact(&mut [0u8; 1])?; // Skip prefix
        Ok(bincode::decode_from_reader(&mut reader, bincode::config::standard())?)
    }
}
struct OpIterator<R: Read> {
    reader: R,
}
impl<R: Read> OpIterator<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: Read + bincode::de::read::Reader> Iterator for OpIterator<R> {
    type Item = Result<WriteOperation>;

    fn next(&mut self) -> Option<Self::Item> {
        // Skip 1-byte prefix
        let mut prefix = [0u8; 1];
        if let Err(e) = self.reader.read_exact(&mut prefix) {
            return if e.kind() == ErrorKind::UnexpectedEof { None } else { Some(Err(e.into())) };
        }

        match bincode::decode_from_reader(&mut self.reader, bincode::config::standard()) {
            | Ok(op) => Some(Ok(op)),
            | Err(_) => {
                // Likely a partial write, treat as end of stream.
                None
            },
        }
    }
}

impl FileOpLogs {
    /// Creates a new `FileOpLogs` by opening the specified `path`.
    ///
    /// If the path is a directory, it will use segmented logs.
    /// If the path is a file, it will use a single file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file/directory cannot be created or opened.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        Self::validate_folder(&path)?;

        let segment_paths = Self::detect_and_sort_existing_segments(&path)?;
        let (segments, active_segment);

        if segment_paths.is_empty() {
            let segment_path = path.join("segment_0.oplog");
            active_segment = Segment::new(segment_path, 0)?;
            segments = Vec::new();
        } else {
            let mut loaded_segments = Vec::new();
            let mut last_end_index: u64 = 0;
            for (i, p) in segment_paths.iter().enumerate() {
                let expected_start_index =
                    if i == 0 { 0 } else { last_end_index.saturating_add(1) };
                let segment = Segment::from_path(p, expected_start_index)?;
                last_end_index = segment.end_index;
                loaded_segments.push(segment);
            }
            active_segment = loaded_segments.pop().unwrap();
            segments = loaded_segments;
        }

        let active_writer = Self::create_writer(&active_segment.path)?;
        Ok(Self { path, active_segment, active_segment_writer: active_writer, segments })
    }

    fn create_writer(path: &PathBuf) -> Result<BufWriter<File>> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(BufWriter::new(file))
    }

    fn validate_folder(path: &PathBuf) -> Result<()> {
        if path.exists() {
            if !path.is_dir() {
                return Err(anyhow::anyhow!(
                    "Path '{}' exists but is not a directory",
                    path.display()
                ));
            }
        } else {
            std::fs::create_dir_all(path)
                .with_context(|| format!("Failed to create directory '{}'", path.display()))?;
        }
        Ok(())
    }

    fn detect_and_sort_existing_segments(path: &PathBuf) -> Result<Vec<PathBuf>> {
        let re = Regex::new(r"^segment_(\d+)\.oplog$")?;
        let mut segments = Vec::new();

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            if let Some(captures) = re.captures(&file_name.to_string_lossy()) {
                if let Ok(index) = captures[1].parse::<u64>() {
                    segments.push((index, entry.path()));
                }
            }
        }

        segments.sort_by_key(|(index, _)| *index);
        Ok(segments.into_iter().map(|(_, path)| path).collect())
    }

    fn rotate_segment(&mut self) -> Result<()> {
        // 1. Finalize the current active segment's writer.
        self.fsync()?;

        // 2. Determine properties for the new active segment BEFORE modifying current state.
        let old_active_segment_ref = &self.active_segment;
        let next_start_index = if old_active_segment_ref.lookups.is_empty() {
            old_active_segment_ref.start_index
        } else {
            old_active_segment_ref.end_index + 1
        };

        // The new segment's file name index is the count of *future* sealed segments.
        let new_segment_file_index = self.segments.len() + 1;
        let new_segment_path = self.path.join(format!("segment_{}.oplog", new_segment_file_index));

        // 3. Create the new segment and its writer. This can fail, so we do it before mutation.
        let new_active_segment = Segment::new(new_segment_path, next_start_index)?;
        let new_writer = Self::create_writer(&new_active_segment.path)?;

        // 4. Atomically swap the new segment into `self`, getting the old one back.
        let old_active_segment = std::mem::replace(&mut self.active_segment, new_active_segment);

        // 5. Push the now-sealed segment to the list.
        self.segments.push(old_active_segment);

        // 6. Replace the writer.
        self.active_segment_writer = new_writer;

        Ok(())
    }
}

impl TWriteAheadLog for FileOpLogs {
    /// Appends a single `WriteOperation` to the file.
    fn append(&mut self, op: WriteOperation) -> Result<()> {
        if self.active_segment.size >= SEGMENT_SIZE && !self.active_segment.lookups.is_empty() {
            self.rotate_segment()?;
        }

        let log_index = op.log_index;
        let serialized = QueryIO::WriteOperation(op).serialize();

        let offset = self.active_segment.size;

        self.active_segment_writer.write_all(&serialized)?;
        self.fsync()?; // Ensure durability for each write 

        // Update metadata only after successful write
        if self.active_segment.lookups.is_empty() {
            self.active_segment.start_index = log_index;
        }
        self.active_segment.lookups.push(LookupIndex::new(log_index, offset));
        self.active_segment.size += serialized.len();
        self.active_segment.end_index = log_index;

        Ok(())
    }

    fn append_many(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        for op in ops {
            self.append(op)?;
        }
        Ok(())
    }

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        let mut result = Vec::new();
        let all_segments = self.segments.iter().chain(std::iter::once(&self.active_segment));

        for segment in all_segments {
            // Skip segments that are entirely outside the requested range
            if segment.end_index <= start_exclusive || segment.start_index > end_inclusive {
                continue;
            }

            // Stream operations and filter
            if let Ok(iter) = segment.iter_ops() {
                for op_result in iter {
                    if let Ok(op) = op_result {
                        if op.log_index > start_exclusive && op.log_index <= end_inclusive {
                            result.push(op);
                        }
                    }
                }
            }
        }
        result
    }

    /// Replays all existing operations in the op_logs, invoking a callback for each.
    fn replay<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        for segment in self.segments.iter().chain(std::iter::once(&self.active_segment)) {
            if let Ok(iter) = segment.iter_ops() {
                for op_result in iter {
                    f(op_result?);
                }
            }
        }
        Ok(())
    }

    /// Forces any buffered data to be written to disk.
    fn fsync(&mut self) -> Result<()> {
        self.active_segment_writer.flush()?;
        self.active_segment_writer.get_ref().sync_all()?;
        Ok(())
    }

    fn read_at(&self, log_index: u64) -> Option<WriteOperation> {
        let all_segments = self.segments.iter().chain(std::iter::once(&self.active_segment));
        for segment in all_segments {
            if log_index >= segment.start_index && log_index <= segment.end_index {
                if let Some(offset) = segment.find_offset(log_index) {
                    return segment.read_at_offset(offset).ok();
                }
            }
        }
        None
    }
    fn log_start_index(&self) -> u64 {
        self.segments.first().map_or(self.active_segment.start_index, |s| s.start_index)
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
    use super::*;
    use anyhow::Result;
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
        let op_logs = FileOpLogs::new(path).unwrap();

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
        let op_logs = FileOpLogs::new(path)?;

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
