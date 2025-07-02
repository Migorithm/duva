use bincode::{
    BorrowDecode,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::domains::caches::cache_objects::{CacheEntry, THasExpiry};

#[derive(Debug, PartialEq, Eq, Clone, Default)]
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

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub enum TypedValue {
    #[default]
    Null,
    String(Bytes),
    List(Vec<Bytes>),
}

impl From<&str> for TypedValue {
    fn from(s: &str) -> Self {
        TypedValue::String(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<Vec<&str>> for TypedValue {
    fn from(v: Vec<&str>) -> Self {
        TypedValue::List(v.into_iter().map(|s| Bytes::copy_from_slice(s.as_bytes())).collect())
    }
}

impl TypedValue {
    pub fn as_bytes(&self) -> anyhow::Result<&[u8]> {
        match self {
            | TypedValue::String(b) => Ok(b.as_ref()),
            | TypedValue::List(_) => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
            | TypedValue::Null => Err(anyhow::anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }
}

impl PartialEq<&str> for TypedValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            | TypedValue::String(b) => b.as_ref() == other.as_bytes(),
            | _ => false,
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
            | TypedValue::Null => 0u8,
            | TypedValue::String(_) => 1u8,
            | TypedValue::List(_) => 2u8,
        };
        bincode::Encode::encode(&kind, encoder)?;
        match &self.value {
            | TypedValue::Null => bincode::Encode::encode(&Vec::<u8>::new(), encoder)?,
            | TypedValue::String(b) => bincode::Encode::encode(&b.to_vec(), encoder)?,
            | TypedValue::List(list) => {
                let vec_of_vec: Vec<Vec<u8>> = list.iter().map(|b| b.to_vec()).collect();
                bincode::Encode::encode(&vec_of_vec, encoder)?
            },
        }
        let expiry_timestamp = self.expiry.map(|dt| dt.timestamp_millis());
        bincode::Encode::encode(&expiry_timestamp, encoder)?;
        Ok(())
    }
}

impl<Ctx> bincode::Decode<Ctx> for CacheValue {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let kind: u8 = bincode::Decode::decode(decoder)?;
        let value = match kind {
            | 0 => TypedValue::Null,
            | 1 => {
                let value_bytes: Vec<u8> = bincode::Decode::decode(decoder)?;
                TypedValue::String(Bytes::from(value_bytes))
            },
            | 2 => {
                let list: Vec<Vec<u8>> = bincode::Decode::decode(decoder)?;
                TypedValue::List(list.into_iter().map(Bytes::from).collect())
            },
            | _ => return Err(DecodeError::Other("Unknown ValueKind variant".into())),
        };
        let expiry_timestamp: Option<i64> = bincode::Decode::decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| DateTime::from_timestamp_millis(ts).unwrap());
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
        let value = match kind {
            | 0 => TypedValue::Null,
            | 1 => {
                let value_bytes: Vec<u8> = BorrowDecode::borrow_decode(decoder)?;
                TypedValue::String(Bytes::from(value_bytes))
            },
            | 2 => {
                let list: Vec<Vec<u8>> = BorrowDecode::borrow_decode(decoder)?;
                TypedValue::List(list.into_iter().map(Bytes::from).collect())
            },
            | _ => return Err(DecodeError::Other("Unknown ValueKind variant")),
        };
        let expiry_timestamp: Option<i64> = BorrowDecode::borrow_decode(decoder)?;
        let expiry = expiry_timestamp.map(|ts| DateTime::from_timestamp_millis(ts).unwrap());
        let value = CacheValue::new(value);
        Ok(match expiry {
            | Some(expiry) => value.with_expiry(expiry),
            | None => value,
        })
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
        assert_eq!(decoded.value, TypedValue::String(Bytes::copy_from_slice(b"hello")));
    }

    #[test]
    fn test_list_variant_encode_decode() {
        let list = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];
        let original = CacheValue { value: TypedValue::List(list.clone()), expiry: None };
        let encoded = encode_to_vec(&original, bincode::config::standard()).unwrap();
        let (decoded, _): (CacheValue, usize) =
            decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.value, TypedValue::List(list));
    }

    #[test]
    fn test_list_variant_from_vec_str() {
        let v = vec!["foo", "bar"];
        let tv = TypedValue::from(v.clone());
        assert_eq!(tv, TypedValue::List(vec![Bytes::from("foo"), Bytes::from("bar")]));
    }

    #[test]
    fn test_as_bytes_returns_err_on_list() {
        let tv = TypedValue::List(vec![Bytes::from("x")]);
        assert!(tv.as_bytes().is_err());
    }
}
