use bincode::{
    BorrowDecode,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::domains::caches::cache_objects::{THasExpiry, types::quicklist::QuickList};

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
            | TypedValue::Null => 0,
            | TypedValue::String(b) => b.len(),
            | TypedValue::List(list) => list.llen(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub enum TypedValue {
    #[default]
    Null,
    String(Bytes),
    List(QuickList),
}

impl From<&str> for TypedValue {
    fn from(s: &str) -> Self {
        TypedValue::String(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl TypedValue {
    pub(crate) fn as_str(&self) -> anyhow::Result<&Bytes> {
        match self {
            | TypedValue::String(b) => Ok(b),
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

impl PartialEq<&str> for CacheValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            | CacheValue { value: TypedValue::String(s), .. } => s.as_ref() == other.as_bytes(),
            | _ => false,
        }
    }
}

impl PartialEq<&[u8]> for CacheValue {
    fn eq(&self, other: &&[u8]) -> bool {
        match self {
            | CacheValue { value: TypedValue::String(s), .. } => s.as_ref() == *other,
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
            | TypedValue::List(list) => bincode::Encode::encode(&list, encoder)?,
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
                let list: QuickList = bincode::Decode::decode(decoder)?;
                TypedValue::List(list)
            },
            | _ => return Err(DecodeError::Other("Unknown ValueKind variant")),
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
                let list: QuickList = BorrowDecode::borrow_decode(decoder)?;
                TypedValue::List(list)
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
