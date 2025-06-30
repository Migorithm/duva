use bincode::{
    BorrowDecode,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::domains::caches::cache_objects::{CacheEntry, THasExpiry};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CacheValue {
    pub(crate) value: TypedValue,
    pub(crate) expiry: Option<DateTime<Utc>>,
}

impl CacheValue {
    pub(crate) fn new(value: impl Into<TypedValue>) -> Self {
        Self { value: value.into(), expiry: None }
    }
    pub(crate) fn with_expiry(self, expiry: DateTime<Utc>) -> Self {
        Self { expiry: Some(expiry), ..self }
    }

    pub(crate) fn value(&self) -> &TypedValue {
        &self.value
    }

    pub(crate) fn to_cache_entry(&self, key: &str) -> CacheEntry {
        CacheEntry { key: key.to_string(), value: self.clone() }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TypedValue {
    String(Bytes),
}

impl From<&str> for TypedValue {
    fn from(s: &str) -> Self {
        TypedValue::String(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl TypedValue {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            | TypedValue::String(b) => b,
        }
    }
    pub fn to_bytes(self) -> Bytes {
        match self {
            | TypedValue::String(b) => b,
        }
    }
}

impl PartialEq<&str> for TypedValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            | TypedValue::String(b) => b.as_ref() == other.as_bytes(),
        }
    }
}

impl THasExpiry for CacheValue {
    fn has_expiry(&self) -> bool {
        self.expiry.is_some()
    }
}

impl bincode::Encode for CacheValue {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let kind = match &self.value {
            | TypedValue::String(_) => 0u8,
        };
        bincode::Encode::encode(&kind, encoder)?;
        bincode::Encode::encode(&self.value.as_bytes(), encoder)?;
        let expiry_timestamp = self.expiry.map(|dt| dt.timestamp_millis());
        bincode::Encode::encode(&expiry_timestamp, encoder)?;
        Ok(())
    }
}

impl<Ctx> bincode::Decode<Ctx> for CacheValue {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let kind: u8 = bincode::Decode::decode(decoder)?;
        let value_bytes: Vec<u8> = bincode::Decode::decode(decoder)?;
        let expiry_timestamp: Option<i64> = bincode::Decode::decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| DateTime::from_timestamp_millis(ts).unwrap());
        let value = match kind {
            | 0 => TypedValue::String(Bytes::from(value_bytes)),

            | _ => return Err(DecodeError::Other("Unknown ValueKind variant".into())),
        };
        let value = CacheValue::new(value);

        Ok(match expiry {
            | Some(expiry) => value.with_expiry(expiry),
            | None => value,
        })
    }
}

impl<'de, Ctx> BorrowDecode<'de, Ctx> for CacheValue {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let kind: u8 = BorrowDecode::borrow_decode(decoder)?;
        let value_bytes: Vec<u8> = BorrowDecode::borrow_decode(decoder)?;
        let expiry_timestamp: Option<i64> = BorrowDecode::borrow_decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| DateTime::from_timestamp_millis(ts).unwrap());
        let value = match kind {
            | 0 => TypedValue::String(Bytes::from(value_bytes)),

            | _ => return Err(DecodeError::Other("Unknown ValueKind variant".into())),
        };
        let value = CacheValue::new(value);

        Ok(match expiry {
            | Some(expiry) => value.with_expiry(expiry),
            | None => value,
        })
    }
}
