use anyhow::Context;
use chrono::{DateTime, Utc};
use std::time::Duration;

use crate::domains::caches::cache_objects::{CacheValue, TypedValue};

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct CacheEntry {
    pub(crate) key: String,
    pub(crate) value: CacheValue,
}

impl CacheEntry {
    pub(crate) fn new(key: impl Into<String>, value: impl Into<TypedValue>) -> Self {
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
            return expiry > current_datetime.timestamp_millis();
        }
        true
    }

    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        self.value.expiry.and_then(DateTime::from_timestamp_millis)
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn from_slice(chunk: &[(&String, &CacheValue)]) -> Vec<Self> {
        chunk
            .iter()
            .map(|(k, v)| CacheEntry::new_with_cache_value(k.to_string(), (*v).clone()))
            .collect::<Vec<CacheEntry>>()
    }

    pub(crate) fn expire_in(&self) -> anyhow::Result<Option<Duration>> {
        if let Some(expiry) = self.expiry() {
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

    pub(crate) fn as_str(&self) -> anyhow::Result<String> {
        self.value.try_to_string()
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
        assert_eq!(decoded_entry.value, original_entry.value);
        assert_eq!(decoded_entry.expiry(), original_entry.expiry());
        assert_eq!(decoded_entry, original_entry);
    }
}
