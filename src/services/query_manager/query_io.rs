use std::time::SystemTime;

use anyhow::Result;

use crate::services::CacheValue;

#[derive(Clone, Debug, PartialEq)]
pub enum QueryIO {
    SimpleString(String),
    BulkString(String),
    Array(Vec<QueryIO>),
    Null,
    Err(String),
}
impl QueryIO {
    pub fn serialize(&self) -> String {
        match self {
            QueryIO::SimpleString(s) => format!("+{}\r\n", s),
            QueryIO::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            QueryIO::Array(a) => {
                let mut result = format!("*{}\r\n", a.len());
                for v in a {
                    result.push_str(&v.serialize());
                }
                result
            }
            QueryIO::Null => "$-1\r\n".to_string(),
            QueryIO::Err(e) => format!("-{}\r\n", e),
        }
    }

    pub fn unpack_bulk_str(self) -> Result<String> {
        match self {
            QueryIO::BulkString(s) => Ok(s.to_lowercase()),
            _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
        }
    }
    pub fn extract_expiry(&self) -> anyhow::Result<SystemTime> {
        match self {
            QueryIO::BulkString(expiry) => {
                let systime = std::time::SystemTime::now()
                    + std::time::Duration::from_millis(expiry.parse::<u64>()?);
                Ok(systime)
            }
            _ => Err(anyhow::anyhow!("Invalid expiry")),
        }
    }
}

impl From<Option<CacheValue>> for QueryIO {
    fn from(v: Option<CacheValue>) -> Self {
        match v {
            Some(CacheValue::Value(v)) => QueryIO::BulkString(v),
            Some(CacheValue::ValueWithExpiry(v, _exp)) => QueryIO::BulkString(v),
            None => QueryIO::Null,
        }
    }
}

impl From<String> for QueryIO {
    fn from(v: String) -> Self {
        QueryIO::BulkString(v)
    }
}
