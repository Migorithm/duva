use crate::services::query_manager::{
    error::IoError,
    interface::{TWrite, TWriterFactory},
};
use tokio::io::AsyncWriteExt;

impl TWrite for tokio::fs::File {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), IoError> {
        AsyncWriteExt::write_all(&mut (self as &mut tokio::fs::File), buf)
            .await
            .map_err(|e| e.kind().into())
    }
}

impl TWriterFactory for tokio::fs::File {
    fn create_writer(
        filepath: String,
    ) -> impl std::future::Future<Output = anyhow::Result<tokio::fs::File>> + Send {
        async move {
            tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(filepath)
                .await
                .map_err(|e| e.into())
        }
    }
}
