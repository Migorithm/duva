use crate::services::statefuls::snapshot::endec::decoder::byte_decoder::BytesDecoder;
use crate::services::statefuls::snapshot::endec::decoder::states::DecoderInit;
use crate::services::statefuls::snapshot::snapshot::Snapshot;

pub struct Load;

pub(crate) struct SnapshotLoader {}

impl SnapshotLoader {
    // Optimization: OS maps the file pages into VM pages and the data is read from the file only when the VM page is accessed.
    // No extra copy is made between kernel and user space
    pub(crate) async fn load_from_filepath(filepath: String) -> anyhow::Result<Snapshot> {
        let file = tokio::fs::File::open(&filepath).await?;
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        Self::load_from_bytes(&mmap)
    }
    pub(crate) fn load_from_bytes(bytes: &[u8]) -> anyhow::Result<Snapshot> {
        let decoder: BytesDecoder<DecoderInit> = bytes.into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        Ok(database)
    }
}
