use anyhow::Context;
use chrono::{DateTime, Utc};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum CacheEntry {
    KeyValue(String, String),
    KeyValueExpiry(String, String, DateTime<Utc>),
}

impl CacheEntry {
    pub(crate) fn is_valid(&self, current_datetime: &DateTime<Utc>) -> bool {
        match &self {
            CacheEntry::KeyValueExpiry(_, _, expiry) => *expiry > *current_datetime,
            _ => true,
        }
    }

    pub(crate) fn expiry(&self) -> Option<DateTime<Utc>> {
        match &self {
            CacheEntry::KeyValueExpiry(_, _, expiry) => Some(*expiry),
            _ => None,
        }
    }

    pub(crate) fn key(&self) -> &str {
        match &self {
            CacheEntry::KeyValue(key, _) => key,
            CacheEntry::KeyValueExpiry(key, _, _) => key,
        }
    }

    pub(crate) fn new(chunk: &[(&String, &CacheValue)]) -> Vec<Self> {
        chunk.iter().map(|(k, v)| v.to_cache_entry(k)).collect::<Vec<CacheEntry>>()
    }

    pub(crate) fn expire_in(&self) -> anyhow::Result<Option<Duration>> {
        if let CacheEntry::KeyValueExpiry(_, _, expiry) = self {
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
    ValueWithExpiry(String, DateTime<Utc>),
}
impl CacheValue {
    pub(crate) fn has_expiry(&self) -> bool {
        matches!(self, CacheValue::ValueWithExpiry(_, _))
    }
    pub(crate) fn value(&self) -> &str {
        match self {
            CacheValue::Value(v) => v,
            CacheValue::ValueWithExpiry(v, _) => v,
        }
    }

    pub(crate) fn to_cache_entry(&self, key: &str) -> CacheEntry {
        match self {
            CacheValue::Value(v) => CacheEntry::KeyValue(key.to_string(), v.clone()),
            CacheValue::ValueWithExpiry(v, expiry) => {
                CacheEntry::KeyValueExpiry(key.to_string(), v.clone(), *expiry)
            },
        }
    }
}
