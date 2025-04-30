use anyhow::Context;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum CacheEntry {
    KeyValue { key: String, value: String },
    KeyValueExpiry { key: String, value: String, expiry: DateTime<Utc> },
    Append { key: String, value: String },
}

impl CacheEntry {
    pub(crate) fn is_valid(&self, current_datetime: &DateTime<Utc>) -> bool {
        match &self {
            CacheEntry::KeyValueExpiry { expiry, .. } => *expiry > *current_datetime,
            _ => true,
        }
    }

    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        match &self {
            CacheEntry::KeyValueExpiry { expiry, .. } => Some(*expiry),
            _ => None,
        }
    }

    pub(crate) fn key(&self) -> &str {
        match &self {
            CacheEntry::KeyValue { key, .. } => key,
            CacheEntry::KeyValueExpiry { key, .. } => key,
            CacheEntry::Append { key, .. } => key,
        }
    }
    pub(crate) fn value(&self) -> &str {
        match &self {
            CacheEntry::KeyValue { value, .. } => value,
            CacheEntry::KeyValueExpiry { value, .. } => value,
        }
    }

    pub(crate) fn new(chunk: &[(&String, &CacheValue)]) -> Vec<Self> {
        chunk.iter().map(|(k, v)| v.to_cache_entry(k)).collect::<Vec<CacheEntry>>()
    }

    pub(crate) fn expire_in(&self) -> anyhow::Result<Option<Duration>> {
        if let CacheEntry::KeyValueExpiry { expiry, .. } = self {
            let dr = expiry
                .signed_duration_since(Utc::now())
                .to_std()
                .context("Expiry time is in the past")?;
            return Ok(Some(dr));
        }
        Ok(None)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct CacheValue {
    pub(crate) value: String,
    pub(crate) expiry: Option<DateTime<Utc>>,
}
impl CacheValue {
    pub(crate) fn new(value: String) -> Self {
        Self { value, expiry: None }
    }
    pub(crate) fn with_expiry(self, expiry: DateTime<Utc>) -> Self {
        Self { expiry: Some(expiry), ..self }
    }
    pub(crate) fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }
    pub(crate) fn value(&self) -> &str {
        &self.value
    }

    pub(crate) fn to_cache_entry(&self, key: &str) -> CacheEntry {
        match self.expiry {
            Some(expiry) => CacheEntry::KeyValueExpiry {
                key: key.to_string(),
                value: self.value.clone(),
                expiry,
            },
            None => CacheEntry::KeyValue { key: key.to_string(), value: self.value.clone() },
        }
    }
}
