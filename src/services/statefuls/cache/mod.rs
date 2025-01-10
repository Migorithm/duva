use std::time::SystemTime;

pub mod actor;
pub mod manager;

pub mod ttl;

#[derive(Debug, Clone)]
pub enum CacheEntry {
    KeyValue(String, String),
    KeyValueExpiry(String, String, SystemTime),
}
impl CacheEntry {
    pub fn is_valid(&self, current_systime: &SystemTime) -> bool {
        match &self {
            CacheEntry::KeyValueExpiry(_, _, expiry) => *expiry > *current_systime,
            _ => true,
        }
    }

    pub fn expiry(&self) -> Option<SystemTime> {
        match &self {
            CacheEntry::KeyValueExpiry(_, _, expiry) => Some(*expiry),
            _ => None,
        }
    }

    pub fn key(&self) -> &str {
        match &self {
            CacheEntry::KeyValue(key, _) => key,
            CacheEntry::KeyValueExpiry(key, _, _) => key,
        }
    }
    pub fn value(&self) -> &str {
        match &self {
            CacheEntry::KeyValue(_, value) => value,
            CacheEntry::KeyValueExpiry(_, value, _) => value,
        }
    }

    pub fn new(chunk: &[(&String, &CacheValue)]) -> Vec<Self> {
        chunk.iter().map(|(k, v)| v.to_cache_entry(k)).collect::<Vec<CacheEntry>>()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CacheValue {
    Value(String),
    ValueWithExpiry(String, SystemTime),
}
impl CacheValue {
    pub fn value(&self) -> &str {
        match self {
            CacheValue::Value(v) => v,
            CacheValue::ValueWithExpiry(v, _) => v,
        }
    }
    pub fn has_expiry(&self) -> bool {
        matches!(self, CacheValue::ValueWithExpiry(_, _))
    }

    pub fn to_cache_entry(&self, key: &str) -> CacheEntry {
        match self {
            CacheValue::Value(v) => CacheEntry::KeyValue(key.to_string(), v.clone()),
            CacheValue::ValueWithExpiry(v, expiry) => {
                CacheEntry::KeyValueExpiry(key.to_string(), v.clone(), *expiry)
            }
        }
    }
}
