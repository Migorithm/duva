use super::cluster::actors::peer::PeerState;
use crate::services::statefuls::cache::CacheValue;
use anyhow::Result;
use bytes::{Bytes, BytesMut};

#[derive(Clone, Debug, PartialEq)]
pub enum QueryIO {
    SimpleString(String),
    BulkString(String),
    Array(Vec<QueryIO>),
    Null,
    Err(String),
    File(Vec<u8>),
    PeerState(PeerState),
}

#[macro_export]
macro_rules! write_array {
    ($($x:expr),*) => {
        crate::services::query_io::QueryIO::Array(vec![$(crate::services::query_io::QueryIO::BulkString($x.into())),*])
    };
}

impl QueryIO {
    pub fn serialize(&self) -> Bytes {
        match self {
            QueryIO::SimpleString(s) => format!("+{}\r\n", s).into(),
            QueryIO::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into(),
            QueryIO::Array(a) => {
                let mut result = format!("*{}\r\n", a.len()).into_bytes();
                for v in a {
                    result.extend(&v.serialize());
                }
                result.into()
            }
            QueryIO::PeerState(PeerState { term, offset, last_updated }) => format!(
                "^\r\n${}\r\n{term}\r\n${}\r\n{offset}\r\n${}\r\n{last_updated}\r\n",
                term.to_string().len(),
                offset.to_string().len(),
                last_updated.to_string().len(),
            )
            .into(),

            QueryIO::Null => "$-1\r\n".into(),
            QueryIO::Err(e) => format!("-{}\r\n", e).into(),
            QueryIO::File(f) => {
                let file_len = f.len() * 2;
                let mut hex_file = String::with_capacity(file_len + file_len.to_string().len() + 2)
                    + format!("${}\r\n", file_len).as_str();

                f.into_iter().for_each(|byte| {
                    hex_file.push_str(&format!("{:02x}", byte));
                });

                hex_file.into()
            }
        }
    }

    pub fn unpack_single_entry<T>(self) -> Result<T>
    where
        T: std::str::FromStr<Err: std::error::Error + Sync + Send + 'static>,
    {
        match self {
            QueryIO::BulkString(s) => Ok(s.to_lowercase().parse::<T>()?.into()),
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

impl From<PeerState> for QueryIO {
    fn from(value: PeerState) -> Self {
        QueryIO::PeerState(value)
    }
}
impl TryFrom<QueryIO> for PeerState {
    type Error = anyhow::Error;

    fn try_from(value: QueryIO) -> std::result::Result<Self, Self::Error> {
        let QueryIO::PeerState(PeerState { term, offset, last_updated }) = value else {
            return Err(anyhow::anyhow!("invalid QueryIO invariant"));
        };

        // SAFETY : failing on this conversion will be a bug. And the system should crash
        Ok(PeerState { term, offset, last_updated })
    }
}

impl From<String> for QueryIO {
    fn from(v: String) -> Self {
        QueryIO::BulkString(v)
    }
}
impl From<Vec<u8>> for QueryIO {
    fn from(v: Vec<u8>) -> Self {
        QueryIO::File(v)
    }
}

pub fn parse(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string_or_file(buffer),
        '^' => parse_peer_state(buffer),
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

    let array_len_in_str = String::from_utf8(line.to_vec())?;
    let len_of_array = array_len_in_str.parse()?;

    let mut bulk_strings = Vec::with_capacity(len_of_array);

    for _ in 0..len_of_array {
        let (value, l) = parse(BytesMut::from(&buffer[len..]))?;
        bulk_strings.push(value);
        len += l;
    }

    Ok((QueryIO::Array(bulk_strings), len))
}

fn parse_peer_state(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    // fixed rule for peer state
    let len = 3;

    let (term, l1) = parse(BytesMut::from(&buffer[len..]))?;
    let (offset, l2) = parse(BytesMut::from(&buffer[len + l1..]))?;
    let (last_updated, l3) = parse(BytesMut::from(&buffer[len + l1 + l2..]))?;

    Ok((
        QueryIO::PeerState(PeerState {
            term: term.unpack_single_entry()?,
            offset: offset.unpack_single_entry()?,
            last_updated: last_updated.unpack_single_entry()?,
        }),
        len + l1 + l2 + l3,
    ))
}

fn parse_bulk_string_or_file(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..]).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;

    let content_len: usize = String::from_utf8(line.to_vec())?.parse()?;

    if let Some((line, _)) = read_until_crlf(&buffer[len..]) {
        // Extract the bulk string from the buffer
        let bulk_string_value = String::from_utf8(line.to_vec())?;
        // Return the bulk string value and adjusted length to account for CRLF
        Ok((QueryIO::BulkString(bulk_string_value), len + content_len + 2))
    } else {
        let file_content = &buffer[len..(len + content_len)];
        let file = file_content
            .chunks(2)
            .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16))
            .collect::<Result<Vec<u8>, _>>()?;
        Ok((QueryIO::File(file), len + content_len))
    }
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    None
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

#[test]
fn test_from_bytes_to_peer_state() {
    // GIVEN
    let buffer = BytesMut::from("^\r\n$3\r\n245\r\n$7\r\n1234329\r\n$8\r\n53999944\r\n");

    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, "^\r\n$3\r\n245\r\n$7\r\n1234329\r\n$8\r\n53999944\r\n".len());
    assert_eq!(
        value,
        QueryIO::PeerState(PeerState { term: 245, offset: 1234329, last_updated: 53999944 })
    );
    let peer_state: PeerState = value.try_into().unwrap();
    assert_eq!(peer_state.term, 245);
    assert_eq!(peer_state.offset, 1234329);
    assert_eq!(peer_state.last_updated, 53999944);
}

#[test]
fn test_from_peer_state_to_bytes() {
    use crate::services::query_io::QueryIO;

    //GIVEN
    let peer_state = PeerState { term: 1, offset: 2, last_updated: 3 };
    //WHEN
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();
    //THEN
    assert_eq!("^\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n", peer_state_serialized);

    //GIVEN
    let peer_state = PeerState { term: 5, offset: 3232, last_updated: 35535300 };
    //WHEN
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();
    //THEN
    assert_eq!("^\r\n$1\r\n5\r\n$4\r\n3232\r\n$8\r\n35535300\r\n", peer_state_serialized);
}

#[test]
fn test_parse_file() {
    // GIVEN
    let file = QueryIO::File(b"hello".to_vec());
    let serialized = file.serialize();
    let buffer = BytesMut::from_iter(serialized);
    // WHEN
    let (value, len) = parse(buffer).unwrap();

    // THEN
    assert_eq!(len, 15);
    assert_eq!(value, QueryIO::File(b"hello".to_vec()));
}
