use crate::services::statefuls::snapshot::endec::decoder::byte_decoder::BytesDecoder;
use crate::services::statefuls::snapshot::endec::decoder::states::DecoderInit;
use crate::services::statefuls::snapshot::snapshot::Snapshot;

pub struct Load;

pub(crate) struct SnapshotLoader {}

impl SnapshotLoader {
    pub(crate) async fn load_from_filepath(filepath: String) -> anyhow::Result<Snapshot> {
        let bytes = tokio::fs::read(filepath).await?;
        Self::load_from_bytes(&bytes)
    }
    pub(crate) fn load_from_bytes(bytes: &[u8]) -> anyhow::Result<Snapshot> {
        let decoder: BytesDecoder<DecoderInit> = bytes.into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}
