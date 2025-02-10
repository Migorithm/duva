//! A local append-only file (AOF) adapter.

use std::path::{Path, PathBuf};

use anyhow::Result;

use bytes::BytesMut;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::services::aof::{TAof, WriteOperation};

/// A local append-only file (AOF) implementation.
pub struct LocalAof {
    /// The file path where the AOF data is stored.
    path: PathBuf,

    /// A buffered writer for the underlying file.
    writer: BufWriter<File>,
}

impl LocalAof {
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

impl TAof for LocalAof {
    /// Appends a single `WriteOperation` to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if writing to or syncing the underlying file fails.
    async fn append(&mut self, op: &WriteOperation) -> Result<()> {
        self.writer.write_all(&op.serialize()).await?;
        self.fsync().await?;

        Ok(())
    }

    /// Replays all existing operations in the AOF, invoking a callback for each.
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

        let ops = WriteOperation::deserialize(bytes)?;

        for op in ops {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_new_creates_aof() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.aof");

        assert!(!path.exists());
        assert!(LocalAof::new(&path).await.is_ok());
        assert!(path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_new_opens_existing_file() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.aof");

        assert!(!path.exists());

        tokio::fs::File::create(&path).await?;
        assert!(path.exists());

        assert!(LocalAof::new(&path).await.is_ok());
        assert!(path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_new_fails_if_directory_not_found() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("invalid/local.aof");

        assert!(!path.exists());
        assert!(LocalAof::new(&path).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_append_stores_to_disk() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.aof");

        let mut aof = LocalAof::new(&path).await?;
        let op = WriteOperation::Set { key: "foo".into(), value: "bar".into() };
        aof.append(&op).await?;
        drop(aof);

        let mut file = tokio::fs::File::open(&path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        assert_eq!(buf, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_replay_multiple_operations() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.aof");

        {
            let mut aof = LocalAof::new(&path).await?;
            aof.append(&WriteOperation::Set { key: "a".into(), value: "a".into() }).await?;
            aof.append(&WriteOperation::Set { key: "b".into(), value: "b".into() }).await?;
            aof.append(&WriteOperation::Set { key: "c".into(), value: "c".into() }).await?;
        }

        let mut aof = LocalAof::new(&path).await?;
        let mut ops = Vec::new();

        aof.replay(|op| {
            ops.push(op);
        })
        .await?;

        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0], WriteOperation::Set { key: "a".into(), value: "a".into() });
        assert_eq!(ops[1], WriteOperation::Set { key: "b".into(), value: "b".into() });
        assert_eq!(ops[2], WriteOperation::Set { key: "c".into(), value: "c".into() });

        Ok(())
    }

    #[tokio::test]
    #[ignore = "This is desired behavior. However, currently deserialize fails if any part of the file is corrupted."]
    async fn test_replay_partial_data() -> Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().join("local.aof");

        // Append three ops.
        {
            let mut aof = LocalAof::new(&path).await?;
            aof.append(&WriteOperation::Set { key: "a".into(), value: "a".into() }).await?;
            aof.append(&WriteOperation::Set { key: "b".into(), value: "b".into() }).await?;
            aof.append(&WriteOperation::Set { key: "c".into(), value: "c".into() }).await?;
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

        let mut aof = LocalAof::new(&path).await?;
        let mut ops = Vec::new();

        assert!(aof
            .replay(|op| {
                ops.push(op);
            })
            .await
            .is_err());

        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0], WriteOperation::Set { key: "a".into(), value: "a".into() });

        Ok(())
    }
}
