use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{
    domains::caches::cache_objects::{THasExpiry, types::quicklist::QuickList},
    types::BinBytes,
};

#[derive(Debug, PartialEq, Eq, Clone, Default, bincode::Encode, bincode::Decode)]
pub struct CacheValue {
    pub(crate) value: TypedValue,
    pub(crate) expiry: Option<i64>,
}

impl CacheValue {
    pub(crate) fn new(value: impl Into<TypedValue>) -> Self {
        Self { value: value.into(), expiry: None }
    }
    pub(crate) fn with_expiry(self, expiry: DateTime<Utc>) -> Self {
        Self { expiry: Some(expiry.timestamp_millis()), ..self }
    }

    pub(crate) fn try_to_string(&self) -> anyhow::Result<String> {
        Ok(String::from_utf8_lossy(self.value.as_str()?).to_string())
    }
    pub(crate) fn is_null(&self) -> bool {
        matches!(self.value, TypedValue::Null)
    }
    pub(crate) fn is_string(&self) -> bool {
        matches!(self.value, TypedValue::String(_))
    }

    pub(crate) fn len(&self) -> usize {
        match &self.value {
            TypedValue::Null => 0,
            TypedValue::String(b) => b.len(),
            TypedValue::List(list) => list.llen(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default, bincode::Encode, bincode::Decode)]
pub enum TypedValue {
    #[default]
    Null,
    String(BinBytes),
    List(QuickList),
}

pub const WRONG_TYPE_ERR_MSG: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";

impl From<&str> for TypedValue {
    fn from(s: &str) -> Self {
        TypedValue::String(Bytes::copy_from_slice(s.as_bytes()).into())
    }
}

impl TypedValue {
    pub(crate) fn as_str(&self) -> anyhow::Result<&Bytes> {
        match self {
            TypedValue::String(b) => Ok(b),
            TypedValue::List(_) => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
            TypedValue::Null => Err(anyhow::anyhow!(WRONG_TYPE_ERR_MSG)),
        }
    }
}

impl THasExpiry for CacheValue {
    fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{decode_from_slice, encode_to_vec};

    #[test]
    fn test_string_variant_encode_decode() {
        let original = CacheValue::new("hello");
        let encoded = encode_to_vec(&original, bincode::config::standard()).unwrap();
        let (decoded, _): (CacheValue, usize) =
            decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.value, TypedValue::String(Bytes::copy_from_slice(b"hello").into()));
    }

    #[test]
    fn test_list_variant_encode_decode() {
        let mut q_list = QuickList::default();

        for i in vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")] {
            q_list.rpush(i);
        }
        let original = CacheValue { value: TypedValue::List(q_list.clone()), expiry: None };
        let encoded = encode_to_vec(&original, bincode::config::standard()).unwrap();
        let (decoded, _): (CacheValue, usize) =
            decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.value, TypedValue::List(q_list));
    }
}
