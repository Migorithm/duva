use std::time::SystemTime;

use query_io::QueryIO;

pub mod config_handler;

pub mod query_io;
pub mod statefuls;

impl From<Option<String>> for QueryIO {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(v) => QueryIO::BulkString(v),
            None => QueryIO::Null,
        }
    }
}
pub enum CacheEntry {
    KeyValue(String, String),
    KeyValueExpiry(String, String, Expiry),
}
impl CacheEntry {
    pub fn is_valid(&self, current_systime: &SystemTime) -> bool {
        match &self {
            CacheEntry::KeyValueExpiry(_, _, expiry) => {
                let expiry = expiry.to_systemtime();
                expiry > *current_systime
            }
            _ => true,
        }
    }
    pub fn is_with_expiry(&self) -> bool {
        match &self {
            CacheEntry::KeyValueExpiry(_, _, _) => true,
            _ => false,
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
}

#[derive(Debug, PartialEq, Eq)]
pub enum Expiry {
    Seconds(u32),
    Milliseconds(u64),
}

impl Expiry {
    pub fn to_systemtime(&self) -> SystemTime {
        match self {
            Expiry::Seconds(secs) => {
                SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(*secs as u64)
            }
            Expiry::Milliseconds(millis) => {
                SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(*millis)
            }
        }
    }
    pub fn to_u64(&self) -> u64 {
        match self {
            Expiry::Seconds(secs) => *secs as u64,
            Expiry::Milliseconds(millis) => *millis,
        }
    }
}
