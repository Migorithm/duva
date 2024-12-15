use crate::services::stream_manager::{
    error::IoError,
    interface::{TWrite, TWriterFactory},
};
use tokio::io::AsyncWriteExt;

impl TWrite for tokio::fs::File {
    async fn write(&mut self, buf: &[u8]) -> Result<(), IoError> {
        AsyncWriteExt::write_all(&mut (self as &mut tokio::fs::File), buf)
            .await
            .map_err(|e| e.kind().into())
    }
}

impl TWriterFactory for tokio::fs::File {
    async fn create_writer(filepath: String) -> anyhow::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath)
            .await
            .map_err(|e| e.into())
    }
}
