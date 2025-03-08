use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::cluster_actors::replication::HeartBeatMessage;
#[cfg(test)]
use crate::domains::cluster_actors::replication::ReplicationInfo;
#[cfg(test)]
use crate::domains::peers::identifier::PeerIdentifier;

use crate::domains::caches::cache_objects::CacheValue;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use std::fmt::Write;

use super::parsing_context::ParseContext;

// ! CURRENTLY, only ascii unicode(0-127) is supported
const FILE_PREFIX: char = '\u{0066}';
const SIMPLE_STRING_PREFIX: char = '+';
const BULK_STRING_PREFIX: char = '$';
const ARRAY_PREFIX: char = '*';
const ERROR_PREFIX: char = '-';
const HEARTBEAT_PREFIX: char = '^';
const REPLICATE_PREFIX: char = '#';
const ACKS_PREFIX: char = '@';

const SERDE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[macro_export]
macro_rules! write_array {
    ($($x:expr),*) => {
        $crate::domains::query_parsers::QueryIO::Array(vec![$($crate::domains::query_parsers::QueryIO::BulkString($x.into())),*])
    };
}

#[derive(Clone, Debug, PartialEq, Default)]
pub enum QueryIO {
    #[default]
    Null,
    SimpleString(String),
    BulkString(String),
    Array(Vec<QueryIO>),
    Err(String),

    // custom types
    File(Bytes),
    HeartBeat(HeartBeatMessage),
    WriteOperation(WriteOperation),
    Acks(Vec<LogIndex>),
}

impl QueryIO {
    pub fn serialize(self) -> Bytes {
        let concatenator = |prefix: char| -> Bytes { Bytes::from_iter([prefix as u8]) };

        match self {
            QueryIO::Null => "$-1\r\n".into(),

            QueryIO::SimpleString(s) => Bytes::from(
                [Bytes::from(SIMPLE_STRING_PREFIX.to_string()), s.into(), Bytes::from("\r\n")]
                    .concat(),
            ),

            QueryIO::BulkString(s) => Bytes::from(
                [
                    Bytes::from(BULK_STRING_PREFIX.to_string()),
                    Bytes::from(s.len().to_string()),
                    Bytes::from("\r\n"),
                    s.into(),
                    Bytes::from("\r\n"),
                ]
                .concat(),
            ),

            QueryIO::File(f) => {
                let file_len = f.len() * 2;
                let mut hex_file = String::with_capacity(file_len + file_len.to_string().len() + 2);

                // * To avoid the overhead of using format! macro by creating intermediate string, use write!
                let _ = write!(&mut hex_file, "{}{}\r\n", FILE_PREFIX, file_len);
                f.into_iter().for_each(|byte| {
                    let _ = write!(hex_file, "{:02x}", byte);
                });

                hex_file.into()
            },

            QueryIO::Array(array) => {
                let mut buffer = BytesMut::with_capacity(
                    // Rough estimate of needed capacity
                    array.len() * 32 + format!("{}{}\r\n", ARRAY_PREFIX, array.len()).len(),
                );

                // extend single buffer
                buffer.extend_from_slice(format!("*{}\r\n", array.len()).as_bytes());
                for item in array {
                    buffer.extend_from_slice(&item.serialize());
                }
                buffer.freeze()
            },

            QueryIO::Err(e) => {
                Bytes::from([concatenator(ERROR_PREFIX), e.into(), "\r\n".into()].concat())
            },

            QueryIO::HeartBeat(heartbeat) => serialize_with_bincode(HEARTBEAT_PREFIX, heartbeat),

            QueryIO::WriteOperation(write_operation) => {
                serialize_with_bincode(REPLICATE_PREFIX, write_operation)
            },
            QueryIO::Acks(items) => serialize_with_bincode(ACKS_PREFIX, items),
        }
    }

    pub fn unpack_single_entry<T>(self) -> Result<T>
    where
        T: std::str::FromStr<Err: std::error::Error + Sync + Send + 'static>,
    {
        match self {
            QueryIO::BulkString(s) => Ok(String::from_utf8(s.into())?.parse::<T>()?),

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

impl From<String> for QueryIO {
    fn from(value: String) -> Self {
        QueryIO::BulkString(value.into())
    }
}
impl From<Vec<String>> for QueryIO {
    fn from(value: Vec<String>) -> Self {
        QueryIO::Array(value.into_iter().map(Into::into).collect())
    }
}
impl From<Option<CacheValue>> for QueryIO {
    fn from(v: Option<CacheValue>) -> Self {
        match v {
            Some(CacheValue::Value(v)) => QueryIO::BulkString(v.into()),
            Some(CacheValue::ValueWithExpiry(v, _exp)) => QueryIO::BulkString(v.into()),
            None => QueryIO::Null,
        }
    }
}

impl From<QueryIO> for Bytes {
    fn from(value: QueryIO) -> Self {
        value.serialize()
    }
}

pub fn deserialize(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    match buffer[0] as char {
        SIMPLE_STRING_PREFIX => {
            let (bytes, len) = parse_simple_string(buffer)?;
            Ok((QueryIO::SimpleString(bytes), len))
        },
        ARRAY_PREFIX => parse_array(buffer),
        BULK_STRING_PREFIX => {
            let (bytes, len) = parse_bulk_string(buffer)?;
            Ok((QueryIO::BulkString(bytes), len))
        },
        FILE_PREFIX => {
            let (bytes, len) = parse_file(buffer)?;
            Ok((QueryIO::File(bytes), len))
        },
        HEARTBEAT_PREFIX => parse_custom_type::<HeartBeatMessage>(buffer),
        REPLICATE_PREFIX => parse_custom_type::<WriteOperation>(buffer),
        ACKS_PREFIX => parse_custom_type::<Vec<LogIndex>>(buffer),

        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

// +PING\r\n
pub(crate) fn parse_simple_string(buffer: BytesMut) -> Result<(String, usize)> {
    let (line, len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid simple string"))?;
    Ok((line, len + 1))
}

fn parse_array(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    let mut ctx = ParseContext::new(buffer);

    // skip array prefix
    ctx.advance(1);

    let (count_bytes, count_len) = read_until_crlf(&BytesMut::from(&ctx.buffer[ctx.offset()..]))
        .ok_or(anyhow::anyhow!("Invalid array length"))?;
    ctx.advance(count_len);

    // Convert array length string to number
    let array_len = count_bytes.parse()?;

    let elements = (0..array_len).map(|_| ctx.parse_next()).collect::<Result<_>>()?;

    Ok((QueryIO::Array(elements), ctx.offset()))
}

pub fn parse_custom_type<T>(
    buffer: BytesMut,
) -> std::result::Result<(QueryIO, usize), anyhow::Error>
where
    T: bincode::Decode<()> + Into<QueryIO>,
{
    let (encoded, len): (T, usize) = bincode::decode_from_slice(&buffer[1..], SERDE_CONFIG)
        .map_err(|err| anyhow::anyhow!("Failed to decode heartbeat message: {:?}", err))?;
    Ok((encoded.into(), len + 1))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(String, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;

    let content_len: usize = line.parse()?;

    let (line, total_len) = read_content_until_crlf(&buffer[len..].into(), content_len)
        .context("Invalid BulkString format!")?;
    Ok((line, len + total_len))
}

fn parse_file(buffer: BytesMut) -> Result<(Bytes, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;
    let content_len: usize = line.parse()?;

    let file_content = &buffer[len..(len + content_len)];

    let file = file_content
        .chunks(2)
        .flat_map(|chunk| std::str::from_utf8(chunk).map(|s| u8::from_str_radix(s, 16)))
        .collect::<Result<Bytes, _>>()?;

    Ok((file, len + content_len))
}
pub(super) fn read_content_until_crlf(
    buffer: &BytesMut,
    content_len: usize,
) -> Option<(String, usize)> {
    if buffer.len() < content_len + 2 {
        return None;
    }
    if buffer[content_len] == b'\r' && buffer[content_len + 1] == b'\n' {
        return Some((
            String::from_utf8_lossy(&buffer[0..content_len]).to_string(),
            content_len + 2,
        ));
    }
    None
}

pub(super) fn read_until_crlf(buffer: &BytesMut) -> Option<(String, usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((String::from_utf8_lossy(&buffer[0..(i - 1)]).to_string(), i + 1));
        }
    }
    None
}

fn serialize_with_bincode<T: bincode::Encode>(prefix: char, arg: T) -> Bytes {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(prefix.to_string().as_bytes());
    buffer.extend_from_slice(&bincode::encode_to_vec(&arg, SERDE_CONFIG).unwrap());
    buffer.freeze()
}

impl From<WriteOperation> for QueryIO {
    fn from(value: WriteOperation) -> Self {
        QueryIO::WriteOperation(value)
    }
}
impl From<Vec<WriteOperation>> for QueryIO {
    fn from(value: Vec<WriteOperation>) -> Self {
        QueryIO::File(
            QueryIO::Array(value.into_iter().map(Into::into).collect::<Vec<_>>()).serialize(),
        )
    }
}

impl From<Vec<LogIndex>> for QueryIO {
    fn from(value: Vec<LogIndex>) -> Self {
        QueryIO::Acks(value)
    }
}
impl From<HeartBeatMessage> for QueryIO {
    fn from(value: HeartBeatMessage) -> Self {
        QueryIO::HeartBeat(value)
    }
}
#[cfg(test)]
mod test {

    use crate::domains::append_only_files::WriteRequest;

    use super::*;

    #[test]
    fn test_parse_simple_string() {
        // GIVEN
        let buffer = BytesMut::from("+OK\r\n");

        // WHEN
        let (value, len) = parse_simple_string(buffer).unwrap();

        // THEN
        assert_eq!(len, 5);
        assert_eq!(value, "OK".to_string());
    }

    #[test]
    fn test_parse_simple_string_ping() {
        // GIVEN
        let buffer = BytesMut::from("+PING\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 7);
        assert_eq!(value, QueryIO::SimpleString("PING".to_string().into()));
    }

    #[test]
    fn test_parse_bulk_string() {
        // GIVEN
        let buffer = BytesMut::from("$5\r\nhello\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 11);
        assert_eq!(value, QueryIO::BulkString("hello".into()));
    }

    #[test]
    fn test_parse_bulk_string_empty() {
        // GIVEN
        let buffer = BytesMut::from("$0\r\n\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 6);
        assert_eq!(value, QueryIO::BulkString("".into()));
    }

    #[test]
    fn test_parse_array() {
        // GIVEN
        let buffer = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 26);
        assert_eq!(
            value,
            QueryIO::Array(vec![
                QueryIO::BulkString("hello".into()),
                QueryIO::BulkString("world".into()),
            ])
        );
    }

    #[test]
    fn test_from_bytes_to_peer_state() {
        // GIVEN

        let data = b"^\x0f127.0.0.1:49153\xf5\xfc\x99\xd5\x12\0\x04abcd\x02\0\0";

        let buffer = BytesMut::from_iter(data);

        // WHEN
        let (QueryIO::HeartBeat(heartbeat), len) = deserialize(buffer).unwrap() else {
            panic!();
        };

        // THEN
        assert_eq!(len, data.len());
        assert_eq!(
            heartbeat,
            HeartBeatMessage {
                term: 245,
                hwm: 1234329,
                leader_replid: "abcd".into(),
                hop_count: 2,
                heartbeat_from: "127.0.0.1:49153".to_string().into(),
                ban_list: vec![],
                append_entries: vec![]
            }
        );

        assert_eq!(heartbeat.term, 245);
        assert_eq!(heartbeat.hwm, 1234329);

        assert_eq!(*heartbeat.leader_replid, "abcd");
        assert_eq!(heartbeat.hop_count, 2);
        assert!(heartbeat.ban_list.is_empty())
    }

    #[test]
    fn test_from_heartbeat_to_bytes() {
        //GIVEN
        let peer_state = HeartBeatMessage {
            term: 1,
            hwm: 2,
            leader_replid: "localhost:3329".into(),
            hop_count: 2,
            heartbeat_from: "127.0.0.1:49152".to_string().into(),
            ban_list: Default::default(),
            append_entries: vec![
                WriteOperation {
                    request: WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                    log_index: 1.into(),
                },
                WriteOperation {
                    request: WriteRequest::Set { key: "poo".into(), value: "bar".into() },
                    log_index: 2.into(),
                },
            ],
        };
        //WHEN
        let peer_state_serialized: QueryIO = peer_state.into();
        let peer_state_serialized = peer_state_serialized.serialize();
        //THEN
        assert_eq!(
            "^\x0f127.0.0.1:49152\x01\x02\x0elocalhost:3329\x02\0\x02\0\x03foo\x03bar\x01\0\x03poo\x03bar\x02",
            peer_state_serialized
        );

        //GIVEN
        let peer_state = HeartBeatMessage {
            term: 5,
            hwm: 3232,
            leader_replid: "localhost:23399".into(),
            hop_count: 40,
            heartbeat_from: "127.0.0.1:49159".to_string().into(),
            ban_list: Default::default(),
            append_entries: vec![],
        };
        //WHEN
        let peer_state_serialized: QueryIO = peer_state.into();
        let peer_state_serialized = peer_state_serialized.serialize();
        //THEN
        assert_eq!(
            b"^\x0f127.0.0.1:49159\x05\xfb\xa0\x0c\x0flocalhost:23399(\0\0".to_vec(),
            peer_state_serialized
        );
    }

    #[test]
    fn test_peer_state_ban_list_to_binary() {
        // GIVEN
        let mut replication = ReplicationInfo::new(None, "127.0.0.1", 6380);
        let peer_id = PeerIdentifier::new("127.0.0.1", 6739);
        replication.ban_peer(&peer_id).unwrap();

        let ban_time = replication.ban_list[0].ban_time;
        // encode ban_time with bincode
        let config = bincode::config::standard();
        let ban_time = bincode::encode_to_vec(ban_time, config).unwrap();

        //WHEN
        let peer_state = replication.default_heartbeat(1);
        let peer_state_serialized: QueryIO = peer_state.into();
        let peer_state_serialized = peer_state_serialized.serialize();

        //THEN
        let expected = Bytes::from(
            [
                Bytes::from_iter(
                    *b"^\x0e127.0.0.1:6380\0\0\x0e127.0.0.1:6380\x01\x01\x0e127.0.0.1:6739",
                ),
                Bytes::from_iter(ban_time),
                Bytes::from_iter(*b"\0"),
            ]
            .concat(),
        );

        assert_eq!(expected, peer_state_serialized);
    }

    #[test]
    fn test_binary_to_heartbeat() {
        // GIVEN
        let buffer = Bytes::from_iter(
            *b"^\x0e127.0.0.1:6379\0\0\x06random\x01\0\x02\0\x03foo\x03bar\x01\0\x03poo\x03bar\x02",
        );

        // WHEN
        let (value, _) = deserialize(buffer.into()).unwrap();

        // THEN
        assert_eq!(
            value,
            QueryIO::HeartBeat(HeartBeatMessage {
                term: 0,
                hwm: 0,
                leader_replid: "random".into(),
                hop_count: 1,
                heartbeat_from: "127.0.0.1:6379".to_string().into(),
                ban_list: [].to_vec(),
                append_entries: vec![
                    WriteOperation {
                        request: WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                        log_index: 1.into()
                    },
                    WriteOperation {
                        request: WriteRequest::Set { key: "poo".into(), value: "bar".into() },
                        log_index: 2.into()
                    }
                ]
            })
        );
    }

    #[test]
    fn test_parse_file() {
        // GIVEN
        let file = QueryIO::File("hello".into());
        let serialized = file.serialize();
        let buffer = BytesMut::from_iter(serialized);
        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(value, QueryIO::File("hello".into()));
    }

    #[test]
    fn test_from_replicate_log_to_binary() {
        // GIVEN
        let query = WriteRequest::Set { key: "foo".into(), value: "bar".into() };
        let replicate =
            QueryIO::WriteOperation(WriteOperation { request: query, log_index: 1.into() });

        // WHEN
        let serialized = replicate.clone().serialize();
        // THEN
        assert_eq!("#\0\x03foo\x03bar\x01", serialized);
    }

    #[test]
    fn test_from_binary_to_replicate_log() {
        // GIVEN
        let data = "#\0\x03foo\x03bar\x01";
        let buffer = BytesMut::from(data);

        // WHEN
        let (value, _) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(
            value,
            QueryIO::WriteOperation(WriteOperation {
                request: WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                log_index: 1.into()
            })
        );
    }

    #[test]
    fn test_from_binary_to_acks() {
        // GIVEN

        let data = "@\x02\x01\x02";
        let buffer = BytesMut::from(data);

        // WHEN
        let (value, _) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(value, QueryIO::Acks(vec![1.into(), 2.into()]));
    }

    #[test]
    fn test_from_acks_to_binary() {
        // GIVEN
        let acks = vec![1.into(), 2.into()];
        let replicate = QueryIO::Acks(acks);

        // WHEN
        let serialized = replicate.clone().serialize();

        // THEN
        assert_eq!("@\x02\x01\x02", serialized);
    }
}
