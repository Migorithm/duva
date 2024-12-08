use crate::services::query_manager::{error::IoError, interface::TWrite};
use tokio::io::AsyncWriteExt;

impl TWrite for tokio::fs::File {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), IoError> {
        AsyncWriteExt::write_all(&mut (self as &mut tokio::fs::File), buf)
            .await
            .map_err(|e| e.kind().into())
    }
}
