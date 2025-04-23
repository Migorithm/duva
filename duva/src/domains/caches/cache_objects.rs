use anyhow::Context;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum CacheEntry {
    KeyValue { key: String, value: String },
    KeyValueExpiry { key: String, value: String, expiry: DateTime<Utc> },
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
pub(crate) enum CacheValue {
    Value(String),
    ValueWithExpiry { value: String, expiry: DateTime<Utc> },
}
impl CacheValue {
    pub(crate) fn has_expiry(&self) -> bool {
        matches!(self, CacheValue::ValueWithExpiry { .. })
    }
    pub(crate) fn value(&self) -> &str {
        match self {
            CacheValue::Value(v) => v,
            CacheValue::ValueWithExpiry { value: v, .. } => v,
        }
    }

    pub(crate) fn to_cache_entry(&self, key: &str) -> CacheEntry {
        match self {
            CacheValue::Value(v) => CacheEntry::KeyValue { key: key.into(), value: v.into() },
            CacheValue::ValueWithExpiry { value: v, expiry } => CacheEntry::KeyValueExpiry {
                key: key.to_string(),
                value: v.clone(),
                expiry: *expiry,
            },
        }
    }
}
