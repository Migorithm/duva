use crate::adapters::persistence::size_encoding::size_decode;
use crate::{from_to, make_smart_pointer, services::statefuls::routers::cache_actor::CacheDb};
use anyhow::Result;
use key_value_storage_extractor::{
    extract_expiry_time_in_milliseconds, extract_expiry_time_in_seconds, KeyValue,
};
use size_encoding::string_decode;
use std::collections::HashMap;
mod database_extractor;
mod key_value_storage_extractor;
pub mod size_encoding;

pub struct RdbFile {
    header: String,
    metadata: HashMap<String, String>,
    database: Vec<CacheDb>,
}

// TODO rename it
struct Data(Vec<u8>);

impl Data {
    fn when_0xFE(&mut self) -> Result<usize> {
        self.remove(0);
        size_decode(self).ok_or(anyhow::anyhow!("size decode fail"))
    }
    fn when_0xFB(&mut self) -> Result<(usize, usize)> {
        self.remove(0);

        let err_mt = || anyhow::anyhow!("size decode fail");
        Ok((
            size_decode(self).ok_or(err_mt())?,
            size_decode(self).ok_or(err_mt())?,
        ))
    }
    fn when_0xFF(&mut self) -> Vec<u8> {
        self.remove(0);
        let checksum = self[0..8].to_vec();
        self.drain(..8);
        checksum
    }

    pub fn when_0xFC(&mut self) -> Result<u64> {
        self.remove(0);
        extract_expiry_time_in_milliseconds(self)
    }
    pub fn when_0xFD(&mut self) -> Result<u64> {
        self.remove(0);
        extract_expiry_time_in_seconds(self)
    }
    pub fn when_0x00(&mut self, mut key_value: KeyValue) -> Result<KeyValue> {
        self.remove(0);
        let key_data: size_encoding::DecodedData =
            string_decode(self).ok_or(anyhow::anyhow!("key decode fail"))?;
        key_value.key = key_data.data;
        let value_data = string_decode(self).ok_or(anyhow::anyhow!("value decode fail"))?;
        key_value.value = value_data.data;
        Ok(key_value)
    }
}
make_smart_pointer!(Data, Vec<u8>);
from_to!(Vec<u8>, Data);
