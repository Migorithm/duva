use std::time::SystemTime;

use crate::services::statefuls::cache::CacheValue;
use anyhow::Result;
use bytes::BytesMut;

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

pub(in crate::services) fn parse(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

// +PING\r\n
pub(crate) fn parse_simple_string(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    let (line, len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid simple string"))?;
    let string = String::from_utf8(line.to_vec())?;
    Ok((QueryIO::SimpleString(string), len + 1))
}

fn parse_array(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    len += 1;

    let len_of_array = TryInto::<usize>::try_into(ConversionWrapper(line))?;
    let mut bulk_strings = Vec::with_capacity(len_of_array);

    for _ in 0..len_of_array {
        let (value, l) = parse(BytesMut::from(&buffer[len..]))?;
        bulk_strings.push(value);
        len += l;
    }

    Ok((QueryIO::Array(bulk_strings), len))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;
    let bulk_str_len = usize::try_from(ConversionWrapper(line))?;

    // Extract the bulk string from the buffer
    let bulk_str = &buffer[len..len + bulk_str_len];
    let bulk_string_value = String::from_utf8(bulk_str.to_vec())?;

    // Return the bulk string value and adjusted length to account for CRLF
    Ok((
        QueryIO::BulkString(bulk_string_value),
        len + bulk_str_len + 2,
    ))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    None
}

pub struct ConversionWrapper<T>(pub(crate) T);

impl TryFrom<ConversionWrapper<&[u8]>> for usize {
    type Error = anyhow::Error;

    fn try_from(value: ConversionWrapper<&[u8]>) -> Result<Self> {
        let string = String::from_utf8(value.0.to_vec())?;
        Ok(string.parse()?)
    }
}

#[test]
fn test_parse_simple_string() {
    // GIVEN
    let buffer = BytesMut::from("+OK\r\n");

    // WHEN
    let (value, len) = parse_simple_string(buffer).unwrap();

    // THEN
    assert_eq!(len, 5);
    assert_eq!(value, QueryIO::SimpleString("OK".to_string()));
}

#[test]
fn test_parse_simple_string_ping() {
    // GIVEN
    let buffer = BytesMut::from("+PING\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 7);
    assert_eq!(value, QueryIO::SimpleString("PING".to_string()));
}

#[test]
fn test_parse_bulk_string() {
    // GIVEN
    let buffer = BytesMut::from("$5\r\nhello\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 11);
    assert_eq!(value, QueryIO::BulkString("hello".to_string()));
}

#[test]
fn test_parse_bulk_string_empty() {
    // GIVEN
    let buffer = BytesMut::from("$0\r\n\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 6);
    assert_eq!(value, QueryIO::BulkString("".to_string()));
}

#[test]
fn test_parse_array() {
    // GIVEN
    let buffer = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 26);
    assert_eq!(
        value,
        QueryIO::Array(vec![
            QueryIO::BulkString("hello".to_string()),
            QueryIO::BulkString("world".to_string()),
        ])
    );
}