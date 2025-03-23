use crate::domains::caches::cache_objects::CacheValue;
use crate::domains::cluster_actors::commands::{ReplicationResponse, RequestVoteReply};
use crate::domains::cluster_actors::heartbeats::heartbeat::{AppendEntriesRPC, ClusterHeartBeat};
use crate::domains::{append_only_files::WriteOperation, cluster_actors::commands::RequestVote};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use std::fmt::Write;
// ! CURRENTLY, only ascii unicode(0-127) is supported
const FILE_PREFIX: char = '\u{0066}';
const SIMPLE_STRING_PREFIX: char = '+';
const BULK_STRING_PREFIX: char = '$';
const ARRAY_PREFIX: char = '*';
const APPEND_ENTRY_RPC_PREFIX: char = '^';
const CLUSTER_HEARTBEAT_PREFIX: char = 'c';
const REPLICATE_PREFIX: char = '#';
const ACKS_PREFIX: char = '@';
const REQUEST_VOTE_PREFIX: char = 'v';
const REQUEST_VOTE_REPLY_PREFIX: char = 'r';
const SERDE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[macro_export]
macro_rules! write_array {
    ($($x:expr),*) => {
        $crate::domains::query_parsers::QueryIO::Array(vec![$($crate::domains::query_parsers::QueryIO::BulkString($x.into())),*])
    };
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryIO {
    Null,
    SimpleString(String),
    BulkString(String),
    Array(Vec<QueryIO>),
    Err(String),

    // custom types
    File(Bytes),
    AppendEntriesRPC(AppendEntriesRPC),
    ClusterHeartBeat(ClusterHeartBeat),
    WriteOperation(WriteOperation),
    ConsensusFollowerResponse(ReplicationResponse),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

impl QueryIO {
    pub fn serialize(self) -> Bytes {
        match self {
            QueryIO::Null => "$-1\r\n".into(),
            QueryIO::SimpleString(s) => {
                let mut buffer =
                    String::with_capacity(SIMPLE_STRING_PREFIX.len_utf8() + s.len() + 2);
                write!(&mut buffer, "{}{}\r\n", SIMPLE_STRING_PREFIX, s).unwrap();
                buffer.into()
            },
            QueryIO::BulkString(s) => {
                let mut byte_mut = BytesMut::with_capacity(1 + 1 + s.len() + 4);
                byte_mut.extend_from_slice(BULK_STRING_PREFIX.encode_utf8(&mut [0; 4]).as_bytes());
                byte_mut.extend_from_slice(s.len().to_string().as_bytes());
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.extend_from_slice(s.as_bytes());
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.freeze()
            },
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
                    array.len() * 32 + 1 + array.len(),
                );

                // extend single buffer
                buffer.extend_from_slice(format!("*{}\r\n", array.len()).as_bytes());
                for item in array {
                    buffer.extend_from_slice(&item.serialize());
                }
                buffer.freeze()
            },
            QueryIO::Err(e) => Bytes::from(["-".to_string(), e.into(), "\r\n".into()].concat()),
            QueryIO::AppendEntriesRPC(heartbeat) => {
                serialize_with_bincode(APPEND_ENTRY_RPC_PREFIX, &heartbeat)
            },
            QueryIO::WriteOperation(write_operation) => {
                serialize_with_bincode(REPLICATE_PREFIX, &write_operation)
            },
            QueryIO::ConsensusFollowerResponse(items) => {
                serialize_with_bincode(ACKS_PREFIX, &items)
            },
            QueryIO::RequestVote(request_vote) => {
                serialize_with_bincode(REQUEST_VOTE_PREFIX, &request_vote)
            },
            QueryIO::RequestVoteReply(request_vote_reply) => {
                serialize_with_bincode(REQUEST_VOTE_REPLY_PREFIX, &request_vote_reply)
            },
            QueryIO::ClusterHeartBeat(heart_beat_message) => {
                serialize_with_bincode(CLUSTER_HEARTBEAT_PREFIX, &heart_beat_message)
            },
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

impl Default for QueryIO {
    fn default() -> Self {
        QueryIO::Null
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
        APPEND_ENTRY_RPC_PREFIX => parse_custom_type::<AppendEntriesRPC>(buffer),
        CLUSTER_HEARTBEAT_PREFIX => parse_custom_type::<ClusterHeartBeat>(buffer),
        REPLICATE_PREFIX => parse_custom_type::<WriteOperation>(buffer),
        ACKS_PREFIX => parse_custom_type::<ReplicationResponse>(buffer),
        REQUEST_VOTE_PREFIX => parse_custom_type::<RequestVote>(buffer),
        REQUEST_VOTE_REPLY_PREFIX => parse_custom_type::<RequestVoteReply>(buffer),

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
    let mut offset = 0;
    offset += 1;

    let (count_bytes, count_len) = read_until_crlf(&BytesMut::from(&buffer[offset..]))
        .ok_or(anyhow::anyhow!("Invalid array length"))?;
    offset += count_len;

    let array_len = count_bytes.parse()?;

    let mut elements = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        let (element, len) = deserialize(BytesMut::from(&buffer[offset..]))?;
        offset += len;
        elements.push(element);
    }

    Ok((QueryIO::Array(elements), offset))
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

fn serialize_with_bincode<T: bincode::Encode>(prefix: char, arg: &T) -> Bytes {
    let prefix_len = prefix.len_utf8();

    // Allocate buffer with reasoanble initial capacity
    let estimated_data_size = 64;
    let mut buffer = BytesMut::with_capacity(prefix_len + estimated_data_size);

    buffer.extend_from_slice(prefix.encode_utf8(&mut [0; 4]).as_bytes());

    let encoded = bincode::encode_to_vec(arg, SERDE_CONFIG).unwrap();
    buffer.extend_from_slice(&encoded);

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

impl From<AppendEntriesRPC> for QueryIO {
    fn from(value: AppendEntriesRPC) -> Self {
        QueryIO::AppendEntriesRPC(value)
    }
}

impl From<ClusterHeartBeat> for QueryIO {
    fn from(value: ClusterHeartBeat) -> Self {
        QueryIO::ClusterHeartBeat(value)
    }
}

impl From<ReplicationResponse> for QueryIO {
    fn from(value: ReplicationResponse) -> Self {
        QueryIO::ConsensusFollowerResponse(value)
    }
}

impl From<RequestVote> for QueryIO {
    fn from(value: RequestVote) -> Self {
        QueryIO::RequestVote(value)
    }
}

impl From<RequestVoteReply> for QueryIO {
    fn from(value: RequestVoteReply) -> Self {
        QueryIO::RequestVoteReply(value)
    }
}
#[cfg(test)]
mod test {
    use crate::domains::cluster_actors::commands::RejectionReason;
    use crate::domains::cluster_actors::replication::{HeartBeatMessage, ReplicationId};
    use crate::domains::peers::identifier::PeerIdentifier;
    use crate::domains::{
        append_only_files::WriteRequest, cluster_actors::replication::BannedPeer,
    };

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
    fn test_parse_file() {
        // GIVEN
        let file = QueryIO::File("hello".into());
        let serialized = file.serialize();
        let buffer = BytesMut::from_iter(serialized);
        // WHEN
        let (value, _) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(value, QueryIO::File("hello".into()));
    }

    #[test]
    fn test_write_operation_to_binary_back_to_itself() {
        // GIVEN
        let op = QueryIO::WriteOperation(WriteOperation {
            request: WriteRequest::Set { key: "foo".into(), value: "bar".into() },
            log_index: 1,
            term: 0,
        });

        // WHEN
        let serialized = op.clone().serialize();
        let (deserialized, _) = deserialize(serialized.clone().into()).unwrap();

        // THEN
        assert_eq!(deserialized, op);
    }

    #[test]
    fn test_acks_to_binary_back_to_acks() {
        // GIVEN
        let follower_res = ReplicationResponse {
            term: 0,
            rej_reason: RejectionReason::None,
            log_idx: 2,
            from: PeerIdentifier("repl1".into()),
        };
        let acks = QueryIO::ConsensusFollowerResponse(follower_res);

        // WHEN
        let serialized = acks.clone().serialize();
        let (deserialized, _) = deserialize(BytesMut::from(serialized)).unwrap();

        // THEN
        assert_eq!(deserialized, acks);
    }

    #[test]
    fn test_heartbeat_to_binary_back_to_heartbeat() {
        // GIVEN
        let me = PeerIdentifier::new("me".into(), 6035);
        let leader = ReplicationId::Undecided;
        let banned_list = vec![
            BannedPeer { p_id: PeerIdentifier("banned1".into()), ban_time: 3553 },
            BannedPeer { p_id: PeerIdentifier("banned2".into()), ban_time: 3556 },
        ];
        let heartbeat = HeartBeatMessage {
            from: me.clone(),
            term: 1,
            prev_log_index: 0,
            prev_log_term: 1,
            hwm: 5,
            replid: leader.clone(),
            hop_count: 2,
            ban_list: banned_list,
            append_entries: vec![
                WriteOperation {
                    request: WriteRequest::Set { key: "foo".into(), value: "bar".into() },
                    log_index: 1,
                    term: 0,
                },
                WriteOperation {
                    request: WriteRequest::SetWithExpiry {
                        key: "foo".into(),
                        value: "bar".into(),
                        expires_at: 323232,
                    },
                    log_index: 2,
                    term: 1,
                },
            ],
            cluster_nodes: vec![
                "127.0.0.1:30004 follower 127.0.0.1:30001".into(),
                "127.0.0.1:30002 ? - 5461-10922".into(),
                "127.0.0.1:30003 ? - 10923-16383".into(),
                "127.0.0.1:30005 follower 127.0.0.1:30002".into(),
                "127.0.0.1:30006 follower 127.0.0.1:30003".into(),
                "127.0.0.1:30001 myself,? - 0-5460".into(),
            ],
        };
        let replicate = QueryIO::AppendEntriesRPC(AppendEntriesRPC(heartbeat));

        // WHEN
        let serialized = replicate.clone().serialize();

        let (value, _) = deserialize(BytesMut::from(serialized)).unwrap();

        // THEN
        assert_eq!(value, replicate);
    }

    #[test]
    fn test_request_vote_to_binary_back_to_request_vote() {
        // GIVEN
        let request_vote = RequestVote {
            term: 1,
            candidate_id: PeerIdentifier("me".into()),
            last_log_index: 5,
            last_log_term: 1,
        };
        let request_vote = QueryIO::RequestVote(request_vote);

        // WHEN
        let serialized = request_vote.clone().serialize();
        let (deserialized, _) = deserialize(BytesMut::from(serialized)).unwrap();

        // THEN
        assert_eq!(deserialized, request_vote);
    }

    #[test]
    fn test_request_vote_reply_to_binary_back_to_request_vote_reply() {
        // GIVEN
        let request_vote_reply = RequestVoteReply { term: 1, vote_granted: true };
        let request_vote_reply = QueryIO::RequestVoteReply(request_vote_reply);

        // WHEN
        let serialized = request_vote_reply.clone().serialize();
        let (deserialized, _) = deserialize(BytesMut::from(serialized)).unwrap();

        // THEN
        assert_eq!(deserialized, request_vote_reply);
    }
}
