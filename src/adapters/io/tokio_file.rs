use crate::services::interface::TWriterFactory;

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
