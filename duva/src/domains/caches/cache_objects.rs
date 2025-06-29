use anyhow::Context;
use bincode::{
    BorrowDecode,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    key: String,
    value: CacheValue,
}

impl CacheEntry {
    pub(crate) fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self { key: key.into(), value: CacheValue::new(value) }
    }

    pub(crate) fn new_with_cache_value(key: impl Into<String>, value: CacheValue) -> Self {
        Self { key: key.into(), value }
    }

    pub(crate) fn with_expiry(self, expiry_millis: DateTime<Utc>) -> CacheEntry {
        CacheEntry { key: self.key, value: self.value.with_expiry(expiry_millis) }
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
    pub(crate) fn value(&self) -> &Bytes {
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
    pub(crate) value: Bytes,
    pub(crate) expiry: Option<DateTime<Utc>>,
}

impl CacheValue {
    pub(crate) fn new(value: impl Into<Bytes>) -> Self {
        Self { value: value.into(), expiry: None }
    }
    pub(crate) fn with_expiry(self, expiry: DateTime<Utc>) -> Self {
        Self { expiry: Some(expiry), ..self }
    }

    pub(crate) fn value(&self) -> &Bytes {
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
        bincode::Encode::encode(&self.value.to_vec(), encoder)?;
        let expiry_timestamp = self.expiry.map(|dt| dt.timestamp_millis());
        bincode::Encode::encode(&expiry_timestamp, encoder)?;
        Ok(())
    }
}

impl<Ctx> bincode::Decode<Ctx> for CacheValue {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let value_bytes: Vec<u8> = bincode::Decode::decode(decoder)?;
        let value = Bytes::from(value_bytes);
        let expiry_timestamp: Option<i64> = bincode::Decode::decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| DateTime::from_timestamp_millis(ts).unwrap());
        Ok(CacheValue { value, expiry })
    }
}

impl<'de, Ctx> BorrowDecode<'de, Ctx> for CacheValue {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let value_bytes: Vec<u8> = BorrowDecode::borrow_decode(decoder)?;
        let value = Bytes::from(value_bytes);
        let expiry_timestamp: Option<i64> = BorrowDecode::borrow_decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| DateTime::from_timestamp_millis(ts).unwrap());
        Ok(CacheValue { value, expiry })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{decode_from_slice, encode_to_vec};
    use chrono::{DateTime, Utc};

    #[test]
    fn test_cache_value_encode_decode_with_expiry() {
        //GIVEN -  Create a CacheValue with expiry (using millisecond precision to match our serialization)
        let expiry_millis = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis();
        let expiry_time = DateTime::from_timestamp_millis(expiry_millis).unwrap();
        let original_value = CacheValue::new("test_value").with_expiry(expiry_time);

        // WHEN
        let encoded = encode_to_vec(&original_value, bincode::config::standard()).unwrap();

        let (decoded_value, _): (CacheValue, usize) =
            decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        // THEN - Verify the decoded value matches the original
        assert_eq!(decoded_value.value, original_value.value);
        assert_eq!(decoded_value.expiry, original_value.expiry);
        assert_eq!(decoded_value, original_value);
    }

    #[test]
    fn test_cache_value_encode_decode_without_expiry() {
        // GIVEN - Create a CacheValue without expiry
        let original_value = CacheValue::new("test_value_no_expiry");

        // Encode the value
        let encoded = encode_to_vec(&original_value, bincode::config::standard()).unwrap();

        // Decode the value back
        let (decoded_value, _): (CacheValue, usize) =
            decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        // Verify the decoded value matches the original
        assert_eq!(decoded_value.value, original_value.value);
        assert_eq!(decoded_value.expiry, None);
        assert_eq!(decoded_value, original_value);
    }

    #[test]
    fn test_cache_entry_encode_decode_with_expiry() {
        // Create a CacheEntry with expiry (using millisecond precision to match our serialization)
        let expiry_millis = (Utc::now() + chrono::Duration::minutes(30)).timestamp_millis();

        let original_entry = CacheEntry::new("test_key", "entry_value")
            .with_expiry(DateTime::from_timestamp_millis(expiry_millis).unwrap());

        // Encode the entry
        let encoded = encode_to_vec(&original_entry, bincode::config::standard()).unwrap();

        // Decode the entry back
        let (decoded_entry, _): (CacheEntry, usize) =
            decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        // Verify the decoded entry matches the original
        assert_eq!(decoded_entry.key(), original_entry.key());
        assert_eq!(decoded_entry.value(), original_entry.value());
        assert_eq!(decoded_entry.expiry(), original_entry.expiry());
        assert_eq!(decoded_entry, original_entry);
    }
}
