use crate::services::statefuls::persistence_models::RdbFile;

use super::ThreadSafeCloneable;

pub trait TDecodeData: ThreadSafeCloneable {
    fn decode_data(&self, bytes: Vec<u8>) -> anyhow::Result<RdbFile>;
}

pub trait TEncodeData: ThreadSafeCloneable {}
