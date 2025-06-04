use anyhow::Context;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug, Clone)]
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
    pub(crate) fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }
    pub(crate) fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn to_cache_entry(&self, key: &str) -> CacheEntry {
        CacheEntry { key: key.to_string(), value: self.clone() }
    }
}
