use crate::adapters::persistence::decoder::byte_decoder::BytesDecoder;
use crate::adapters::persistence::decoder::states::DecoderInit;
use crate::adapters::persistence::encoder::encoding_processor::{EncodingMeta, EncodingProcessor};
use crate::services::interfaces::endec::{TDecodeData, TEncodeData};

#[derive(Default, Debug, Clone)]
pub struct EnDecoder;

impl TDecodeData for EnDecoder {
    fn decode_data(
        &self,
        bytes: Vec<u8>,
    ) -> anyhow::Result<crate::services::statefuls::persistence_models::RdbFile> {
        let decoder: BytesDecoder<DecoderInit> = bytes.as_slice().into();
        let database = decoder.load_header()?.load_metadata()?.load_database()?;
        print!("database: {:?}", database);
        Ok(database)
    }
}

#[allow(refining_impl_trait)]
impl TEncodeData for EnDecoder {
    async fn create_on_path(
        &self,
        filepath: &str,
        num_of_cache_actors: usize,
    ) -> anyhow::Result<EncodingProcessor> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filepath)
            .await?;
        Ok(EncodingProcessor {
            file,
            meta: EncodingMeta::new(num_of_cache_actors),
        })
    }
}