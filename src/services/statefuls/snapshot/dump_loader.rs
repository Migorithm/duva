use super::endec::decoder::byte_decoder::BytesDecoder;
use super::endec::decoder::states::DecoderInit;
use crate::services::statefuls::snapshot::dump_file::DumpFile;

pub struct Load;

pub(crate) struct DumpLoader {}

impl DumpLoader {
    pub(crate) async fn load(filepath: String) -> anyhow::Result<DumpFile> {
        let bytes = tokio::fs::read(filepath).await?;
        let decoder: BytesDecoder<DecoderInit> = bytes.as_slice().into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}
