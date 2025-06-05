use anyhow::Context;
use bincode::{
    BorrowDecode,
    error::{DecodeError, EncodeError},
};
use chrono::{DateTime, TimeZone, Utc};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct CacheEntry {
    key: String,
    value: CacheValue,
}

impl CacheEntry {
    pub(crate) fn new(key: String, value: CacheValue) -> Self {
        Self { key, value }
    }

    pub(crate) fn is_valid(&self, current_datetime: &DateTime<Utc>) -> bool {
        if let Some(expiry) = self.value.expiry {
            return expiry > *current_datetime;
        }
        true
    }

    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        self.value.expiry
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }
    pub(crate) fn value(&self) -> &str {
        self.value.value()
    }

    pub(crate) fn from_slice(chunk: &[(&String, &CacheValue)]) -> Vec<Self> {
        chunk.iter().map(|(k, v)| v.to_cache_entry(k)).collect::<Vec<CacheEntry>>()
    }

    pub(crate) fn expire_in(&self) -> anyhow::Result<Option<Duration>> {
        if let Some(expiry) = self.value.expiry {
            let dr = expiry
                .signed_duration_since(Utc::now())
                .to_std()
                .context("Expiry time is in the past")?;
            return Ok(Some(dr));
        }
        Ok(None)
    }

    pub(crate) fn destructure(self) -> (String, CacheValue) {
        (self.key, self.value)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CacheValue {
    pub(crate) value: String,
    pub(crate) expiry: Option<DateTime<Utc>>,
}

impl CacheValue {
    pub(crate) fn new(value: String) -> Self {
        Self { value, expiry: None }
    }
    pub(crate) fn with_expiry(self, expiry: Option<DateTime<Utc>>) -> Self {
        Self { expiry, ..self }
    }

    pub(crate) fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn to_cache_entry(&self, key: &str) -> CacheEntry {
        CacheEntry { key: key.to_string(), value: self.clone() }
    }
}

pub(crate) trait THasExpiry {
    fn has_expiry(&self) -> bool;
}

impl THasExpiry for CacheValue {
    fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }
}

impl bincode::Encode for CacheEntry {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        bincode::Encode::encode(&self.key, encoder)?;
        bincode::Encode::encode(&self.value, encoder)?;
        Ok(())
    }
}

impl<Ctx> bincode::Decode<Ctx> for CacheEntry {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let key: String = bincode::Decode::decode(decoder)?;
        let value: CacheValue = bincode::Decode::decode(decoder)?;
        Ok(CacheEntry { key, value })
    }
}

impl<'de, Ctx> BorrowDecode<'de, Ctx> for CacheEntry {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let key: String = BorrowDecode::borrow_decode(decoder)?;
        let value: CacheValue = BorrowDecode::borrow_decode(decoder)?;
        Ok(CacheEntry { key, value })
    }
}

impl bincode::Encode for CacheValue {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        bincode::Encode::encode(&self.value, encoder)?;
        let expiry_timestamp = self.expiry.map(|dt| dt.timestamp());
        bincode::Encode::encode(&expiry_timestamp, encoder)?;
        Ok(())
    }
}

impl<Ctx> bincode::Decode<Ctx> for CacheValue {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let value: String = bincode::Decode::decode(decoder)?;
        let expiry_timestamp: Option<i64> = bincode::Decode::decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| Utc.timestamp_opt(ts, 0).single().unwrap());
        Ok(CacheValue { value, expiry })
    }
}

impl<'de, Ctx> BorrowDecode<'de, Ctx> for CacheValue {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let value: String = BorrowDecode::borrow_decode(decoder)?;
        let expiry_timestamp: Option<i64> = BorrowDecode::borrow_decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| Utc.timestamp_opt(ts, 0).single().unwrap());
        Ok(CacheValue { value, expiry })
    }
}
