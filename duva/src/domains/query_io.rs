use crate::domains::caches::cache_objects::{CacheValue, TypedValue};
use crate::domains::replications::*;
use crate::types::BinBytes;
use anyhow::{Context, Result, anyhow};

use bincode::enc::write::SizeWriter;
use bytes::{Bytes, BytesMut};

// ! CURRENTLY, only ascii unicode(0-127) is supported

const BULK_STRING_PREFIX: char = '$';
const ARRAY_PREFIX: char = '*';
const NULL_PREFIX: char = '\u{0000}';
pub const SERDE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[macro_export]
macro_rules! write_array {
    ($($x:expr),*) => {
        $crate::domains::QueryIO::Array(vec![$($crate::domains::QueryIO::BulkString(BinBytes::new($x))),*])
    };
}

#[derive(Clone, Debug, PartialEq, Default, bincode::Decode, bincode::Encode)]
pub enum QueryIO {
    #[default]
    Null,
    BulkString(BinBytes),
    Array(Vec<QueryIO>),
}

impl QueryIO {
    pub fn serialize(self) -> BinBytes {
        match self {
            QueryIO::Null => BinBytes(NULL_PREFIX.to_string().into()),

            QueryIO::BulkString(s) => {
                let mut byte_mut = BytesMut::with_capacity(1 + 1 + s.len() + 4);
                byte_mut.extend_from_slice(BULK_STRING_PREFIX.encode_utf8(&mut [0; 4]).as_bytes());
                byte_mut.extend_from_slice(s.len().to_string().as_bytes());
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.extend_from_slice(&s);
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.freeze().into()
            },

            QueryIO::Array(array) => {
                // Better capacity estimation: header + sum of item sizes
                let header_size = 1 + array.len().to_string().len() + 2; // prefix + len + \r\n
                let estimated_item_size = array.iter().map(estimate_serialized_size).sum::<usize>();
                let mut buffer = BytesMut::with_capacity(header_size + estimated_item_size);

                // Write array header directly
                buffer.extend_from_slice(&[ARRAY_PREFIX as u8]);
                buffer.extend_from_slice(array.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");

                for item in array {
                    buffer.extend_from_slice(&item.serialize());
                }
                buffer.freeze().into()
            },
        }
    }

    pub fn unpack_single_entry<T>(self) -> Result<T>
    where
        T: std::str::FromStr<Err: std::error::Error + Sync + Send + 'static>,
    {
        match self {
            QueryIO::BulkString(s) => {
                let string_value = String::from_utf8(s.to_vec())?;
                Ok(string_value.parse::<T>()?)
            },
            _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
        }
    }

    pub fn unpack_array<T>(self) -> Result<Vec<T>>
    where
        T: std::str::FromStr<Err: std::error::Error + Sync + Send + 'static>,
    {
        let QueryIO::Array(s) = self else {
            return Err(anyhow::anyhow!("Expected command to be a bulk array"));
        };

        let mut result = vec![];
        for v in s {
            let temp_val = v.unpack_single_entry()?;
            result.push(temp_val);
        }
        Ok(result)
    }

    pub fn merge(self, other: QueryIO) -> Result<QueryIO> {
        match (self, other) {
            (QueryIO::Array(mut a), QueryIO::Array(b)) => {
                a.extend(b);
                Ok(QueryIO::Array(a))
            },
            (QueryIO::Null, a) | (a, QueryIO::Null) => Ok(a),
            (QueryIO::Array(mut a), b) => {
                a.push(b);
                Ok(QueryIO::Array(a))
            },
            (a, QueryIO::Array(mut b)) => {
                b.push(a);
                Ok(QueryIO::Array(b))
            },
            _ => Err(anyhow!("Only Arrays can be merged")),
        }
    }

    pub(crate) fn convert_str_vec_res(values: Vec<String>, index: u64) -> Self {
        QueryIO::Array(values.into_iter().map(|v| QueryIO::BulkString(BinBytes::new(v))).collect())
    }
}

pub(crate) fn serialized_len_with_bincode<T: bincode::Encode>(prefix: char, arg: &T) -> usize {
    let prefix_len = prefix.len_utf8();
    let mut size_writer = SizeWriter::default();
    bincode::encode_into_writer(arg, &mut size_writer, SERDE_CONFIG).unwrap();
    prefix_len + size_writer.bytes_written
}

fn estimate_serialized_size(query: &QueryIO) -> usize {
    match query {
        QueryIO::Null => 1,
        QueryIO::BulkString(s) => 1 + s.len().to_string().len() + 2 + s.len() + 2,
        QueryIO::Array(array) => {
            let header = 1 + array.len().to_string().len() + 2;
            let items: usize = array.iter().map(estimate_serialized_size).sum();
            header + items
        },
    }
}

impl From<String> for QueryIO {
    fn from(value: String) -> Self {
        QueryIO::BulkString(BinBytes::new(value))
    }
}
impl From<Vec<String>> for QueryIO {
    fn from(value: Vec<String>) -> Self {
        QueryIO::Array(value.into_iter().map(Into::into).collect())
    }
}
impl From<CacheValue> for QueryIO {
    fn from(v: CacheValue) -> Self {
        match v {
            CacheValue { value: TypedValue::Null, .. } => QueryIO::Null,
            CacheValue { value: TypedValue::String(s), .. } => QueryIO::BulkString(s),
            // TODO rendering full list at once is not supported yet
            CacheValue { value: TypedValue::List(_b), .. } => {
                panic!("List is not supported");
            },
        }
    }
}

impl From<Option<String>> for QueryIO {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(v) => QueryIO::BulkString(BinBytes::new(v)),
            None => QueryIO::Null,
        }
    }
}

impl From<QueryIO> for BinBytes {
    fn from(value: QueryIO) -> Self {
        value.serialize()
    }
}

pub fn deserialize(buffer: impl Into<Bytes>) -> Result<(QueryIO, usize)> {
    let buffer: Bytes = buffer.into();
    if buffer.is_empty() {
        return Err(anyhow::anyhow!("Empty buffer"));
    }
    let prefix = buffer[0] as char;
    deserialize_by_prefix(buffer, prefix)
}

fn deserialize_by_prefix(buffer: Bytes, prefix: char) -> Result<(QueryIO, usize)> {
    match prefix {
        ARRAY_PREFIX => parse_array(buffer),

        BULK_STRING_PREFIX => {
            let (bytes, len) = parse_bulk_string(buffer)?;
            Ok((QueryIO::BulkString(BinBytes(bytes)), len))
        },

        NULL_PREFIX => Ok((QueryIO::Null, 1)),
        _ => Err(anyhow::anyhow!("Unknown value type with prefix: {:?}", prefix)),
    }
}

fn parse_array(buffer: Bytes) -> Result<(QueryIO, usize)> {
    // Skip the array type indicator (first byte)
    let mut offset = 1;

    let (count_bytes, count_len) = read_until_crlf_exclusive(&buffer.slice(offset..))
        .ok_or(anyhow::anyhow!("Invalid array length"))?;
    offset += count_len;

    let array_len = count_bytes.parse()?;

    let mut elements = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        let (element, len) = deserialize(buffer.slice(offset..))?;
        offset += len;
        elements.push(element);
    }

    Ok((QueryIO::Array(elements), offset))
}

fn parse_bulk_string(buffer: Bytes) -> Result<(Bytes, usize)> {
    let (line, len) = read_until_crlf_exclusive(&buffer.slice(1..))
        .ok_or(anyhow::anyhow!("Invalid bulk string"))?;
    let content_len: usize = line.parse().context("Invalid bulk string length")?;

    let content_start = len + 1;
    let content_end = content_start + content_len;
    if content_end + 2 > buffer.len() {
        return Err(anyhow::anyhow!("Incomplete bulk string"));
    }

    if &buffer[content_end..content_end + 2] != b"\r\n" {
        return Err(anyhow::anyhow!("Invalid bulk string terminator"));
    }

    Ok((buffer.slice(content_start..content_end), content_end + 2))
}

/// None if crlf not found.
#[inline]
pub(super) fn read_until_crlf_exclusive(buffer: &Bytes) -> Option<(String, usize)> {
    memchr::memmem::find(buffer, b"\r\n").map(|i| {
        // Directly convert to String if valid UTF-8, otherwise use lossy conversion
        let slice = &buffer[0..i];
        let string = match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(slice).to_string(),
        };
        (string, i + 2)
    })
}

pub fn serialize_with_bincode<T: bincode::Encode>(prefix: char, arg: &T) -> BinBytes {
    // Use size estimation for better performance
    let estimated_size = serialized_len_with_bincode(prefix, arg);
    let mut buffer = BytesMut::with_capacity(estimated_size);

    buffer.extend_from_slice(prefix.encode_utf8(&mut [0; 4]).as_bytes());

    let encoded = bincode::encode_to_vec(arg, SERDE_CONFIG).unwrap();
    buffer.extend_from_slice(&encoded);

    buffer.freeze().into()
}

impl From<()> for QueryIO {
    fn from(_: ()) -> Self {
        QueryIO::Null
    }
}

// Add From implementations for custom types that need to be converted to Bytes
impl From<ReplicationId> for Bytes {
    fn from(value: ReplicationId) -> Self {
        value.to_string().into()
    }
}

impl From<ReplicationRole> for Bytes {
    fn from(value: ReplicationRole) -> Self {
        value.to_string().into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_bulk_string() {
        // GIVEN
        let buffer = Bytes::from("$5\r\nhello\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 11);
        assert_eq!(value, QueryIO::BulkString(BinBytes::new("hello")));
    }

    #[test]
    fn test_deserialize_bulk_string_empty() {
        // GIVEN
        let buffer = Bytes::from("$0\r\n\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 6);
        assert_eq!(value, QueryIO::BulkString(BinBytes::new("")));
    }

    #[test]
    fn test_deserialize_array() {
        // GIVEN
        let buffer = Bytes::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 26);
        assert_eq!(
            value,
            QueryIO::Array(vec![
                QueryIO::BulkString(BinBytes::new("hello")),
                QueryIO::BulkString(BinBytes::new("world")),
            ])
        );
    }

    #[test]
    fn test_merge_arrays() {
        // GIVEN
        let array1 = QueryIO::Array(vec![
            QueryIO::BulkString(BinBytes::new("a")),
            QueryIO::BulkString(BinBytes::new("b")),
        ]);
        let array2 = QueryIO::Array(vec![
            QueryIO::BulkString(BinBytes::new("c")),
            QueryIO::BulkString(BinBytes::new("d")),
        ]);

        // WHEN
        let merged = array1.merge(array2).unwrap();

        // THEN
        assert_eq!(
            merged,
            QueryIO::Array(vec![
                QueryIO::BulkString(BinBytes::new("a")),
                QueryIO::BulkString(BinBytes::new("b")),
                QueryIO::BulkString(BinBytes::new("c")),
                QueryIO::BulkString(BinBytes::new("d"))
            ])
        );
    }

    #[test]
    fn test_merge_array_with_single_element() {
        // GIVEN
        let array = QueryIO::Array(vec![QueryIO::BulkString(BinBytes::new("a"))]);
        let single_element = QueryIO::BulkString(BinBytes::new("b"));

        // WHEN
        let merged = array.merge(single_element).unwrap();

        // THEN
        assert_eq!(
            merged,
            QueryIO::Array(vec![
                QueryIO::BulkString(BinBytes::new("a")),
                QueryIO::BulkString(BinBytes::new("b"))
            ])
        );
    }

    #[test]
    fn test_merge_null_with_array() {
        // GIVEN
        let array = QueryIO::Array(vec![QueryIO::BulkString(BinBytes::new("a"))]);
        let null_value = QueryIO::Null;

        // WHEN
        let merged = array.clone().merge(null_value).unwrap();

        // THEN
        assert_eq!(merged, array);
    }
}
