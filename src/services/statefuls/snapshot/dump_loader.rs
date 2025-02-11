use super::endec::decoder::byte_decoder::BytesDecoder;
use super::endec::decoder::states::DecoderInit;
use crate::services::statefuls::snapshot::dump_file::DumpFile;

pub struct Load;

pub(crate) struct DumpLoader {}

impl DumpLoader {
    pub(crate) async fn load_from_filepath(filepath: String) -> anyhow::Result<DumpFile> {
        let bytes = tokio::fs::read(filepath).await?;
        Self::load_from_bytes(bytes.as_slice())
    }

    pub(crate) fn load_from_bytes(bytes: &[u8]) -> anyhow::Result<DumpFile> {
        let decoder: BytesDecoder<DecoderInit> = bytes.into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}
