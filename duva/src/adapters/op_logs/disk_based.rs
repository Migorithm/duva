#![allow(dead_code, unused_variables)]
use crate::domains::operation_logs::interfaces::TWriteAheadLog;
use crate::domains::operation_logs::{WriteOperation, WriteRequest};
use anyhow::{Context, Result};
use bytes::Bytes;
use regex::Regex;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

const SEGMENT_SIZE: usize = 1024 * 1024; // 1MB per segment

/// A local write-ahead-log file (WAL) implementation using segmented logs.
pub struct FileOpLogs {
    /// The directory where all segment files are stored, or the file path if not using segments
    path: PathBuf,
    active_segment: Segment,
    segments: Vec<Segment>,
}

#[derive(Clone)]
struct Segment {
    path: PathBuf,
    start_index: u64,
    end_index: u64,
    size: usize,
}

impl Segment {
    async fn new(path: PathBuf) -> Self {
        let _file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await
            .context(format!("Failed to create initial segment '{}'", path.display()))
            .unwrap();

        Self { path, start_index: 0, end_index: 0, size: 0 }
    }

    async fn from_path(path: &PathBuf) -> Result<Self> {
        // Read the last segment file to get its metadata
        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .context(format!("Failed to open segment '{}'", path.display()))?;

        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;

        // Parse the segment file to get operations
        let bytes = Bytes::copy_from_slice(&buf[..]);
        let operations = WriteRequest::deserialize(bytes)?;

        // Calculate segment metadata
        let size = buf.len();

        Ok(Segment {
            path: path.clone(),
            start_index: operations.first().map(|op| op.log_index).unwrap_or(0),
            end_index: operations.last().map(|op| op.log_index).unwrap_or(0),
            size,
        })
    }

    async fn create_writer(&self) -> Result<BufWriter<File>> {
        let file = OpenOptions::new().create(true).append(true).read(true).open(&self.path).await?;
        Ok(BufWriter::new(file))
    }
}

impl FileOpLogs {
    /// Creates a new `FileOpLogs` by opening the specified `path`.
    /// If the path is a directory, it will use segmented logs.
    /// If the path is a file, it will use a single file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file/directory cannot be created or opened.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        Self::validate_folder(&path).await?;

        // Detect and sort existing segment files
        let segment_paths = Self::detect_and_sort_existing_segments(&path).await?;

        let active_segment =
            Self::take_last_segment_otherwise_init(&path, segment_paths.clone()).await?;

        // Load all segments except the last one (which is the active segment)
        let mut segments = Vec::new();
        for segment_path in segment_paths.iter().take(segment_paths.len().saturating_sub(1)) {
            let segment = Segment::from_path(segment_path).await?;
            segments.push(segment);
        }

        Ok(Self { path, active_segment, segments })
    }
    async fn validate_folder(path: &PathBuf) -> Result<(), anyhow::Error> {
        Ok(match tokio::fs::metadata(path).await {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(anyhow::anyhow!(
                        "Path '{}' exists but is not a directory",
                        path.display()
                    ));
                }
            },
            Err(e) if e.kind() == ErrorKind::NotFound => {
                tokio::fs::create_dir_all(path)
                    .await
                    .context(format!("Failed to create directory '{}'", path.display()))?;
            },
            Err(e) => {
                return Err(e).context(format!("Failed to access path '{}'", path.display()));
            },
        })
    }

    async fn detect_and_sort_existing_segments(
        path: &PathBuf,
    ) -> Result<Vec<PathBuf>, anyhow::Error> {
        // Ensure the directory exists before trying to read it
        if !tokio::fs::try_exists(path).await? {
            return Err(anyhow::anyhow!("Directory does not exist: {:?}", path));
        }
        // Compile regex once outside the loop for better performance
        let re = Regex::new(r"^segment_(\d+)\.oplog$")?;

        // Collect and process entries in one pass
        let mut segments = Vec::new();
        let mut read_dir = tokio::fs::read_dir(path).await?;

        while let Some(entry) = read_dir.next_entry().await? {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Since we know the regex will match, we can simplify the capture extraction
            if let Some(captures) = re.captures(&file_name_str) {
                // By rule, we know this unwrap is safe
                let index = captures[1].parse::<u64>().unwrap();
                segments.push((index, entry.path()));
            }
        }

        // Sort segments by index
        segments.sort_by_key(|(index, _)| *index);

        // Extract just the paths
        Ok(segments.into_iter().map(|(_, path)| path).collect())
    }

    async fn take_last_segment_otherwise_init(
        path: &PathBuf,
        segment_paths: Vec<PathBuf>,
    ) -> Result<Segment, anyhow::Error> {
        let active_segment = if segment_paths.is_empty() {
            // No segments exist — create initial segment
            let segment_path = path.join("segment_0.oplog");
            let segment = Segment::new(segment_path).await;
            segment
        } else {
            // Segments exist — use the last one as active
            let segment = Segment::from_path(segment_paths.last().unwrap()).await?;
            segment
        };
        Ok(active_segment)
    }

    async fn rotate_segment(&mut self) -> Result<()> {
        // Close current segment
        if let Some(mut writer) = self.active_segment.create_writer().await.ok() {
            writer.flush().await?;
            writer.get_mut().sync_all().await?;
        }

        // Add to segments list
        self.segments.push(self.active_segment.clone());

        // Create new segment
        let next_index = self.segments.len();
        let segment_path = self.path.join(format!("segment_{}.oplog", next_index));
        let _ = OpenOptions::new().create(true).append(true).read(true).open(&segment_path).await?;

        self.active_segment = Segment {
            path: segment_path,
            start_index: self.active_segment.end_index + 1,
            end_index: self.active_segment.end_index,
            size: 0,
        };

        Ok(())
    }
}

impl TWriteAheadLog for FileOpLogs {
    /// Appends a single `WriteOperation` to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if writing to or syncing the underlying file fails.
    async fn append(&mut self, op: WriteOperation) -> Result<()> {
        // Check if we need to rotate
        if self.active_segment.size >= SEGMENT_SIZE {
            self.rotate_segment().await?;
        }

        // Write operation to current segment
        let log_index = op.log_index;
        let serialized = op.serialize();

        let mut writer = self.active_segment.create_writer().await?;
        writer.write_all(&serialized).await?;
        writer.flush().await?;
        writer.get_mut().sync_all().await?;

        self.active_segment.size += serialized.len();
        self.active_segment.end_index = log_index;

        Ok(())
    }

    async fn append_many(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        for op in ops {
            self.append(op).await?;
        }
        Ok(())
    }

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        let result = Vec::new();

        // Find segments that contain the range
        for segment in &self.segments {
            if segment.end_index >= start_exclusive && segment.start_index <= end_inclusive {
                // TODO: Implement reading from segment file
            }
        }

        // Check active segment

        if self.active_segment.end_index >= start_exclusive
            && self.active_segment.start_index <= end_inclusive
        {
            // TODO: Implement reading from active segment
        }

        result
    }

    /// Replays all existing operations in the WAL, invoking a callback for each.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or deserializing from the file fails.
    async fn replay<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(WriteOperation) + Send,
    {
        // Replay all segments in order
        for segment in &self.segments {
            let file = OpenOptions::new().read(true).open(&segment.path).await?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await?;

            let bytes = Bytes::copy_from_slice(&buf[..]);
            for op in WriteRequest::deserialize(bytes)? {
                f(op);
            }
        }

        // Replay active segment

        let file = OpenOptions::new().read(true).open(&self.active_segment.path).await?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;

        let bytes = Bytes::copy_from_slice(&buf[..]);
        for op in WriteRequest::deserialize(bytes)? {
            f(op);
        }

        Ok(())
    }

    /// Forces any buffered data to be written to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if either flush or sync fails.
    async fn fsync(&mut self) -> Result<()> {
        if let Some(mut writer) = self.active_segment.create_writer().await.ok() {
            writer.flush().await?;
            writer.get_mut().sync_all().await?;
        }

        Ok(())
    }

    async fn follower_full_sync(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        Ok(())
    }

    async fn read_at(&self, prev_log_index: u64) -> Option<WriteOperation> {
        // Find the segment containing the index
        for segment in &self.segments {
            if segment.start_index <= prev_log_index && segment.end_index >= prev_log_index {
                // TODO: Implement reading specific operation from segment
            }
        }

        // Check active segment
        if self.active_segment.start_index <= prev_log_index
            && self.active_segment.end_index >= prev_log_index
        {
            // TODO: Implement reading specific operation from active segment
        }

        None
    }

    fn log_start_index(&self) -> u64 {
        if let Some(first_segment) = self.segments.first() {
            first_segment.start_index
        } else {
            self.active_segment.start_index
        }
    }

    fn is_empty(&self) -> bool {
        self.segments.is_empty() && self.active_segment.size == 0
    }

    async fn truncate_after(&mut self, log_index: u64) {
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

    #[tokio::test]
    async fn test_new_creates_aof() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        assert!(!path.exists());
        assert!(FileOpLogs::new(&path).await.is_ok());

        // THEN
        assert!(path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_append_stores_to_disk() {
        // GIVEN
        let dir = TempDir::new().unwrap();
        let path = dir.path();
        let mut wal = FileOpLogs::new(&path).await.unwrap();
        let request = WriteRequest::Set { key: "foo".into(), value: "bar".into() };
        let write_op = WriteOperation { request, log_index: 0, term: 0 };

        // WHEN
        wal.append(write_op).await.unwrap();
        drop(wal);

        // THEN
        let mut file = tokio::fs::File::open(&path.join("segment_0.oplog")).await.unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();

        let (encoded, _): (WriteOperation, usize) =
            bincode::decode_from_slice(&buf[1..], bincode::config::standard()).unwrap();

        let key = match encoded.request {
            WriteRequest::Set { key, .. } => key,
            WriteRequest::SetWithExpiry { key, .. } => key,
            WriteRequest::Delete { keys: key } => key[0].clone(),
        };
        assert_eq!(key, "foo");
    }

    #[tokio::test]
    async fn test_replay_multiple_operations() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        {
            let mut op_logs = FileOpLogs::new(&path).await?;
            op_logs
                .append(WriteOperation {
                    request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                    log_index: 0,
                    term: 0,
                })
                .await?;
            op_logs
                .append(WriteOperation {
                    request: WriteRequest::Set { key: "b".into(), value: "b".into() },
                    log_index: 1,
                    term: 0,
                })
                .await?;
            op_logs
                .append(WriteOperation {
                    request: WriteRequest::Set { key: "c".into(), value: "c".into() },
                    log_index: 2,
                    term: 1,
                })
                .await?;
        }

        let mut op_logs = FileOpLogs::new(&path).await?;
        let mut ops = Vec::new();

        op_logs
            .replay(|op| {
                ops.push(op);
            })
            .await?;

        // THEN
        assert_eq!(ops.len(), 3);
        assert_eq!(
            ops[0],
            WriteOperation {
                request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                log_index: 0,
                term: 0
            }
        );
        assert_eq!(
            ops[1],
            WriteOperation {
                request: WriteRequest::Set { key: "b".into(), value: "b".into() },
                log_index: 1,
                term: 0
            }
        );
        assert_eq!(
            ops[2],
            WriteOperation {
                request: WriteRequest::Set { key: "c".into(), value: "c".into() },
                log_index: 2,
                term: 1
            }
        );

        Ok(())
    }

    #[tokio::test]
    #[ignore = "This is desired behavior. However, currently deserialize fails if any part of the file is corrupted."]
    async fn test_replay_partial_data() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path().join("local.oplog");

        // WHEN
        // Append three ops.
        {
            let mut op_logs = FileOpLogs::new(&path).await?;
            op_logs
                .append(WriteOperation {
                    request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                    log_index: 0,
                    term: 0,
                })
                .await?;
            op_logs
                .append(WriteOperation {
                    request: WriteRequest::Set { key: "b".into(), value: "b".into() },
                    log_index: 1,
                    term: 0,
                })
                .await?;
            op_logs
                .append(WriteOperation {
                    request: WriteRequest::Set { key: "c".into(), value: "c".into() },
                    log_index: 2,
                    term: 1,
                })
                .await?;
        }

        // Corrupt file content by truncating to the first half.
        // We should only have one complete op.
        {
            let mut file = OpenOptions::new().read(true).open(&path).await?;
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;

            data.truncate(data.len() / 2);

            let mut file = OpenOptions::new().write(true).truncate(true).open(&path).await?;
            file.write_all(&data).await?;
        }

        let mut op_logs = FileOpLogs::new(&path).await?;
        let mut ops = Vec::new();

        assert!(
            op_logs
                .replay(|op| {
                    ops.push(op);
                })
                .await
                .is_err()
        );

        // THEN
        assert_eq!(ops.len(), 1);
        assert_eq!(
            ops[0],
            WriteOperation {
                request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                log_index: 0,
                term: 0
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_new_creates_initial_segment() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // WHEN
        let wal = FileOpLogs::new(&path).await?;

        // THEN

        let segment = wal.active_segment;
        assert_eq!(segment.start_index, 0);
        assert_eq!(segment.end_index, 0);
        assert_eq!(segment.size, 0);
        assert!(segment.path.exists());
        assert!(segment.path.ends_with("segment_0.oplog"));

        Ok(())
    }

    #[tokio::test]
    async fn test_new_loads_existing_segment_metadata() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create initial segment with some operations
        {
            let mut wal = FileOpLogs::new(&path).await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                log_index: 10,
                term: 1,
            })
            .await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "b".into(), value: "b".into() },
                log_index: 11,
                term: 1,
            })
            .await?;
        }

        // WHEN
        let op_logs = FileOpLogs::new(&path).await?;

        // THEN

        let segment = op_logs.active_segment;
        assert_eq!(segment.start_index, 10);
        assert_eq!(segment.end_index, 11);
        assert!(segment.size > 0);
        assert!(segment.path.exists());
        assert!(segment.path.ends_with("segment_0.oplog"));

        // Verify we can read the operations
        let file = OpenOptions::new().read(true).open(&segment.path).await?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        let bytes = Bytes::copy_from_slice(&buf[..]);
        let operations = WriteRequest::deserialize(bytes)?;
        assert_eq!(operations.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_new_handles_multiple_segments() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create multiple segments by forcing rotation
        {
            let mut wal = FileOpLogs::new(&path).await?;
            // Fill first segment
            for i in 0..100 {
                wal.append(WriteOperation {
                    request: WriteRequest::Set {
                        key: format!("key_{}", i).into(),
                        value: format!("value_{}", i).into(),
                    },
                    log_index: i as u64,
                    term: 1,
                })
                .await?;
            }
            // Force rotation
            wal.rotate_segment().await?;
            // Add to new segment
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "new".into(), value: "value".into() },
                log_index: 100,
                term: 1,
            })
            .await?;
        }

        // WHEN
        let mut op_logs = FileOpLogs::new(&path).await?;

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
        op_logs.replay(|op| ops.push(op)).await?;
        assert_eq!(ops.len(), 101);

        Ok(())
    }

    #[tokio::test]
    async fn test_new_handles_corrupted_segment() -> Result<()> {
        // GIVEN
        let dir = TempDir::new()?;
        let path = dir.path();

        // Create a segment and corrupt it
        let mut wal = FileOpLogs::new(&path).await?;
        wal.append(WriteOperation {
            request: WriteRequest::Set { key: "good".into(), value: "data".into() },
            log_index: 0,
            term: 1,
        })
        .await?;

        // Corrupt the segment file
        let segment_path = path.join("segment_0.oplog");
        let mut file = OpenOptions::new().write(true).open(&segment_path).await?;
        file.write_all(b"corrupted data").await?;

        // WHEN/THEN
        assert!(FileOpLogs::new(&path).await.is_err());

        Ok(())
    }
}
