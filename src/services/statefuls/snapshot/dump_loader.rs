use anyhow::Error;
use bytes::Bytes;
use super::endec::decoder::byte_decoder::BytesDecoder;
use super::endec::decoder::states::DecoderInit;
use crate::services::statefuls::snapshot::dump_file::DumpFile;

pub struct Load;

pub(crate) struct DumpLoader {}

impl DumpLoader {
    pub(crate) async fn load_from_filepath(filepath: String) -> anyhow::Result<DumpFile> {
        let bytes:Bytes = tokio::fs::read(filepath).await?.into();
        Self::load_from_bytes(bytes)
    }

    pub(crate) fn load_from_bytes(bytes: Bytes) -> anyhow::Result<DumpFile> {
        let decoder: BytesDecoder<DecoderInit> = bytes.into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}
