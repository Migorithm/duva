use crate::domains::append_only_files::interfaces::TWriteAheadLog;
use crate::domains::append_only_files::{WriteOperation, WriteRequest};
use anyhow::Result;
use bytes::BytesMut;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

/// A local write-ahead-log file (WAL) implementation.

pub struct LocalWAL {
    /// The file path where the WAL data is stored.
    path: PathBuf,

    /// A buffered writer for the underlying file.
    writer: BufWriter<File>,
}

impl LocalWAL {
    /// Creates a new `LocalAof` by opening the specified `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or opened.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).read(true).open(&path_buf).await?;

        Ok(Self { path: path_buf, writer: BufWriter::new(file) })
    }
}

impl TWriteAheadLog for LocalWAL {
    /// Appends a single `WriteOperation` to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if writing to or syncing the underlying file fails.
    async fn append(&mut self, op: WriteOperation) -> Result<()> {
        self.writer.write_all(&op.serialize()).await?;
        self.fsync().await?;

        Ok(())
    }

    async fn append_many(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        // merge all operations into a single buffer
        let mut buf = BytesMut::new();
        for op in ops {
            buf.extend_from_slice(&op.clone().serialize());
        }

        // write all operations to the file
        self.writer.write_all(&buf).await?;

        Ok(())
    }

    fn range(&self, start_exclusive: u64, end_inclusive: u64) -> Vec<WriteOperation> {
        todo!()
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
        let file = OpenOptions::new().read(true).open(&self.path).await?;
        let mut reader = BufReader::new(file);

        let mut buf = Vec::new();

        reader.read_to_end(&mut buf).await?;

        let bytes = BytesMut::from(&buf[..]);

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
        // Flush any buffered data.
        self.writer.flush().await?;

        // Force data write to persistent storage.
        let file = self.writer.get_mut();
        file.sync_all().await?;

        Ok(())
    }

    // TODO create tmp file then rename
    async fn overwrite(&mut self, ops: Vec<WriteOperation>) -> Result<()> {
        todo!()
    }

    async fn read_at(&self, prev_log_index: u64) -> Option<WriteOperation> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_new_creates_aof() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.wal");

        assert!(!path.exists());
        assert!(LocalWAL::new(&path).await.is_ok());
        assert!(path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_new_opens_existing_file() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.wal");

        assert!(!path.exists());

        tokio::fs::File::create(&path).await?;
        assert!(path.exists());

        assert!(LocalWAL::new(&path).await.is_ok());
        assert!(path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_new_fails_if_directory_not_found() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("invalid/local.wal");

        assert!(!path.exists());
        assert!(LocalWAL::new(&path).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_append_stores_to_disk() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.wal");

        let mut wal = LocalWAL::new(&path).await?;

        let request = WriteRequest::Set { key: "foo".into(), value: "bar".into() };
        let write_op = WriteOperation { request, log_index: 0, term: 0 };
        wal.append(write_op).await?;
        drop(wal);

        let mut file = tokio::fs::File::open(&path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        let (encoded, _): (WriteOperation, usize) =
            bincode::decode_from_slice(&buf[1..], bincode::config::standard()).unwrap();

        assert_eq!(encoded.request.key(), "foo");

        Ok(())
    }

    #[tokio::test]
    async fn test_replay_multiple_operations() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.wal");

        {
            let mut wal = LocalWAL::new(&path).await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                log_index: 0,
                term: 0,
            })
            .await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "b".into(), value: "b".into() },
                log_index: 1,
                term: 0,
            })
            .await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "c".into(), value: "c".into() },
                log_index: 2,
                term: 1,
            })
            .await?;
        }

        let mut wal = LocalWAL::new(&path).await?;
        let mut ops = Vec::new();

        wal.replay(|op| {
            ops.push(op);
        })
        .await?;

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
        let dir = TempDir::new()?;
        let path = dir.path().join("local.wal");

        // Append three ops.
        {
            let mut wal = LocalWAL::new(&path).await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "a".into(), value: "a".into() },
                log_index: 0,
                term: 0,
            })
            .await?;
            wal.append(WriteOperation {
                request: WriteRequest::Set { key: "b".into(), value: "b".into() },
                log_index: 1,
                term: 0,
            })
            .await?;
            wal.append(WriteOperation {
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

        let mut wal = LocalWAL::new(&path).await?;
        let mut ops = Vec::new();

        assert!(
            wal.replay(|op| {
                ops.push(op);
            })
            .await
            .is_err()
        );

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
}
