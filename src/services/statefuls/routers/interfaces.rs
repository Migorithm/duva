use crate::services::statefuls::persistence_models::RdbFile;

pub trait TDecodeData: 'static + Send + Sync + Clone {
    fn decode_data(&self, bytes: Vec<u8>) -> anyhow::Result<RdbFile>;
}
