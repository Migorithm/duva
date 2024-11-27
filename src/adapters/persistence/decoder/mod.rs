use byte_decoder::BytesDecoder;
use states::DecoderInit;

use crate::services::statefuls::routers::interfaces::TDecodeData;

mod builder;
pub mod byte_decoder;
pub mod states;

#[derive(Default, Debug, Clone)]
pub struct Decoder;

impl TDecodeData for Decoder {
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
