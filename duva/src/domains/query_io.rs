use crate::domains::caches::cache_objects::{CacheValue, TypedValue};
use crate::domains::cluster_actors::topology::Topology;
use crate::domains::peers::command::{BatchEntries, BatchId, HeartBeat};

use crate::domains::replications::WriteOperation;
use crate::domains::replications::messages::{ElectionVote, ReplicationAck, RequestVote};

use crate::domains::replications::*;
use crate::presentation::clients::request::ClientAction;
use anyhow::{Context, Result, anyhow};

use bincode::enc::write::SizeWriter;
use bytes::{Bytes, BytesMut};

// ! CURRENTLY, only ascii unicode(0-127) is supported
const FILE_PREFIX: char = '\u{0066}';
const SIMPLE_STRING_PREFIX: char = '+';
const BULK_STRING_PREFIX: char = '$';
const ARRAY_PREFIX: char = '*';
const APPEND_ENTRY_RPC_PREFIX: char = '^';
const CLUSTER_HEARTBEAT_PREFIX: char = 'c';
const TOPOLOGY_CHANGE_PREFIX: char = 't';
const START_REBALANCE_PREFIX: char = 'T';
pub const WRITE_OP_PREFIX: char = '#';
const ACKS_PREFIX: char = '@';
const REQUEST_VOTE_PREFIX: char = 'v';
const REQUEST_VOTE_REPLY_PREFIX: char = 'r';
const SESSION_REQUEST_PREFIX: char = '!';
const MIGRATE_BATCH_PREFIX: char = 'm';
const MIGRATION_BATCH_ACK_PREFIX: char = 'M';
const CLOSE_CONNECTION_PREFIX: char = 'C';

const ERR_PREFIX: char = '-';
const NULL_PREFIX: char = '\u{0000}';
const CLIENT_ACTION_PREFIX: char = '%';
pub(crate) const SERDE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[macro_export]
macro_rules! write_array {
    ($($x:expr),*) => {
        $crate::domains::QueryIO::Array(vec![$($crate::domains::QueryIO::BulkString($x.into())),*])
    };
}

#[derive(Clone, Debug, PartialEq, Default)]
pub enum QueryIO {
    #[default]
    Null,
    SimpleString(Bytes),
    BulkString(Bytes),
    Array(Vec<QueryIO>),
    SessionRequest {
        request_id: u64,
        action: ClientAction,
    },
    Err(Bytes),

    // custom types
    File(Bytes),
    AppendEntriesRPC(HeartBeat),
    ClusterHeartBeat(HeartBeat),
    WriteOperation(WriteOperation),
    Ack(ReplicationAck),
    RequestVote(RequestVote),
    RequestVoteReply(ElectionVote),

    TopologyChange(Topology),
    StartRebalance,
    MigrateBatch(BatchEntries),
    MigrationBatchAck(BatchId),
    CloseConnection,
}

impl QueryIO {
    pub fn serialize(self) -> Bytes {
        match self {
            QueryIO::Null => NULL_PREFIX.to_string().into(),
            QueryIO::SimpleString(s) => {
                let mut buffer =
                    BytesMut::with_capacity(SIMPLE_STRING_PREFIX.len_utf8() + s.len() + 2);
                buffer.extend_from_slice(&[SIMPLE_STRING_PREFIX as u8]);
                buffer.extend_from_slice(&s);
                buffer.extend_from_slice(b"\r\n");
                buffer.freeze()
            },
            QueryIO::BulkString(s) => {
                let mut byte_mut = BytesMut::with_capacity(1 + 1 + s.len() + 4);
                byte_mut.extend_from_slice(BULK_STRING_PREFIX.encode_utf8(&mut [0; 4]).as_bytes());
                byte_mut.extend_from_slice(s.len().to_string().as_bytes());
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.extend_from_slice(&s);
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.freeze()
            },
            QueryIO::File(f) => {
                let file_len = f.len() * 2;
                let header_len = FILE_PREFIX.len_utf8() + file_len.to_string().len() + 2;
                let mut buffer = BytesMut::with_capacity(header_len + file_len);

                // Write header directly to buffer
                buffer.extend_from_slice(FILE_PREFIX.encode_utf8(&mut [0; 4]).as_bytes());
                buffer.extend_from_slice(file_len.to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");

                // Write hex bytes directly to buffer without format!
                for byte in f.iter() {
                    let high = (byte >> 4) & 0x0f;
                    let low = byte & 0x0f;
                    buffer.extend_from_slice(&[if high < 10 {
                        b'0' + high
                    } else {
                        b'a' + high - 10
                    }]);
                    buffer.extend_from_slice(&[if low < 10 {
                        b'0' + low
                    } else {
                        b'a' + low - 10
                    }]);
                }

                buffer.freeze()
            },
            QueryIO::Array(array) => {
                // Better capacity estimation: header + sum of item sizes
                let header_size = 1 + array.len().to_string().len() + 2; // prefix + len + \r\n
                let estimated_item_size =
                    array.iter().map(|item| estimate_serialized_size(item)).sum::<usize>();
                let mut buffer = BytesMut::with_capacity(header_size + estimated_item_size);

                // Write array header directly
                buffer.extend_from_slice(&[ARRAY_PREFIX as u8]);
                buffer.extend_from_slice(array.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");

                for item in array {
                    buffer.extend_from_slice(&item.serialize());
                }
                buffer.freeze()
            },
            QueryIO::SessionRequest { request_id, action } => {
                let value = serialize_with_bincode(CLIENT_ACTION_PREFIX, &action);
                let mut buffer = BytesMut::with_capacity(32 + 1 + value.len());
                buffer.extend_from_slice(&[SESSION_REQUEST_PREFIX as u8]);
                buffer.extend_from_slice(request_id.to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer.extend_from_slice(&value);
                buffer.freeze()
            },
            QueryIO::Err(e) => {
                let mut buffer = BytesMut::with_capacity(ERR_PREFIX.len_utf8() + e.len() + 2);
                buffer.extend_from_slice(&[ERR_PREFIX as u8]);
                buffer.extend_from_slice(&e);
                buffer.extend_from_slice(b"\r\n");
                buffer.freeze()
            },
            QueryIO::AppendEntriesRPC(heartbeat) => {
                serialize_with_bincode(APPEND_ENTRY_RPC_PREFIX, &heartbeat)
            },
            QueryIO::WriteOperation(write_operation) => {
                serialize_with_bincode(WRITE_OP_PREFIX, &write_operation)
            },
            QueryIO::Ack(items) => serialize_with_bincode(ACKS_PREFIX, &items),
            QueryIO::RequestVote(request_vote) => {
                serialize_with_bincode(REQUEST_VOTE_PREFIX, &request_vote)
            },
            QueryIO::RequestVoteReply(request_vote_reply) => {
                serialize_with_bincode(REQUEST_VOTE_REPLY_PREFIX, &request_vote_reply)
            },
            QueryIO::ClusterHeartBeat(heart_beat_message) => {
                serialize_with_bincode(CLUSTER_HEARTBEAT_PREFIX, &heart_beat_message)
            },
            QueryIO::TopologyChange(topology) => {
                serialize_with_bincode(TOPOLOGY_CHANGE_PREFIX, &topology)
            },
            QueryIO::StartRebalance => serialize_with_bincode(START_REBALANCE_PREFIX, &()),
            QueryIO::MigrateBatch(migrate_batch) => {
                serialize_with_bincode(MIGRATE_BATCH_PREFIX, &migrate_batch)
            },
            QueryIO::MigrationBatchAck(migration_batch_ack) => {
                serialize_with_bincode(MIGRATION_BATCH_ACK_PREFIX, &migration_batch_ack)
            },
            QueryIO::CloseConnection => CLOSE_CONNECTION_PREFIX.to_string().into(),
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
        QueryIO::SimpleString(s) => 1 + s.len() + 2,
        QueryIO::BulkString(s) => 1 + s.len().to_string().len() + 2 + s.len() + 2,
        QueryIO::Array(array) => {
            let header = 1 + array.len().to_string().len() + 2;
            let items: usize = array.iter().map(estimate_serialized_size).sum();
            header + items
        },
        QueryIO::File(f) => {
            let file_len = f.len() * 2;
            1 + file_len.to_string().len() + 2 + file_len
        },
        QueryIO::Err(e) => 1 + e.len() + 2,
        QueryIO::SessionRequest { request_id, .. } => {
            1 + request_id.to_string().len() + 2 + 64 // rough estimate for action
        },
        // For custom types, use a reasonable default
        _ => 128,
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
impl From<CacheValue> for QueryIO {
    fn from(v: CacheValue) -> Self {
        match v {
            CacheValue { value: TypedValue::Null, .. } => QueryIO::Null,
            CacheValue { value: TypedValue::String(s), .. } => QueryIO::BulkString(s.into()),
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
            Some(v) => QueryIO::BulkString(v.into()),
            None => QueryIO::Null,
        }
    }
}

impl From<QueryIO> for Bytes {
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
        SIMPLE_STRING_PREFIX => {
            let (bytes, len) = parse_simple_string(buffer)?;
            Ok((QueryIO::SimpleString(bytes), len))
        },
        ARRAY_PREFIX => parse_array(buffer),
        SESSION_REQUEST_PREFIX => parse_session_request(buffer),
        BULK_STRING_PREFIX => {
            let (bytes, len) = parse_bulk_string(buffer)?;
            Ok((QueryIO::BulkString(bytes), len))
        },
        FILE_PREFIX => {
            let (bytes, len) = parse_file(buffer)?;
            Ok((QueryIO::File(bytes), len))
        },
        ERR_PREFIX => {
            let (bytes, len) = parse_simple_string(buffer)?;
            Ok((QueryIO::Err(bytes), len))
        },
        NULL_PREFIX => Ok((QueryIO::Null, 1)),

        APPEND_ENTRY_RPC_PREFIX => {
            let (heartbeat, len) = parse_heartbeat(buffer)?;
            Ok((QueryIO::AppendEntriesRPC(heartbeat), len))
        },
        CLUSTER_HEARTBEAT_PREFIX => {
            let (heartbeat, len) = parse_heartbeat(buffer)?;
            Ok((QueryIO::ClusterHeartBeat(heartbeat), len))
        },
        WRITE_OP_PREFIX => parse_custom_type::<WriteOperation>(buffer),
        ACKS_PREFIX => parse_custom_type::<ReplicationAck>(buffer),
        REQUEST_VOTE_PREFIX => parse_custom_type::<RequestVote>(buffer),
        REQUEST_VOTE_REPLY_PREFIX => parse_custom_type::<ElectionVote>(buffer),
        TOPOLOGY_CHANGE_PREFIX => parse_custom_type::<Topology>(buffer),
        START_REBALANCE_PREFIX => Ok((QueryIO::StartRebalance, 1)),
        MIGRATE_BATCH_PREFIX => parse_custom_type::<BatchEntries>(buffer),
        MIGRATION_BATCH_ACK_PREFIX => parse_custom_type::<BatchId>(buffer),
        CLOSE_CONNECTION_PREFIX => Ok((QueryIO::CloseConnection, 1)),
        _ => Err(anyhow::anyhow!("Unknown value type with prefix: {:?}", prefix)),
    }
}

// +PING\r\n
pub(crate) fn parse_simple_string(buffer: Bytes) -> Result<(Bytes, usize)> {
    let (line, len) = read_until_crlf_exclusive(&buffer.slice(1..))
        .ok_or(anyhow::anyhow!("Invalid simple string"))?;
    Ok((line.into(), len + 1))
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

fn parse_session_request(buffer: Bytes) -> Result<(QueryIO, usize)> {
    // Skip the array type indicator (first byte)
    let mut offset = 1;

    let (count_bytes, count_len) = read_until_crlf_exclusive(&buffer.slice(offset..))
        .ok_or(anyhow::anyhow!("Invalid array length"))?;
    offset += count_len;
    let request_id = count_bytes.parse()?;

    let (action_byte, len): (ClientAction, usize) = parse_client_action(buffer.slice(offset..))?;
    Ok((QueryIO::SessionRequest { request_id, action: action_byte }, offset + len))
}

fn parse_client_action(buffer: Bytes) -> Result<(ClientAction, usize)> {
    let (action, len): (ClientAction, usize) =
        bincode::decode_from_slice(&buffer.slice(1..), SERDE_CONFIG)
            .map_err(|err| anyhow::anyhow!("Failed to decode client action: {:?}", err))?;
    Ok((action, len + 1))
}

pub fn parse_custom_type<T>(buffer: Bytes) -> Result<(QueryIO, usize)>
where
    T: bincode::Decode<()> + Into<QueryIO>,
{
    let (encoded, len): (T, usize) =
        bincode::decode_from_slice(&buffer.slice(1..), SERDE_CONFIG)
            .map_err(|err| anyhow::anyhow!("Failed to decode heartbeat message: {:?}", err))?;
    Ok((encoded.into(), len + 1))
}

fn parse_heartbeat(buffer: Bytes) -> Result<(HeartBeat, usize)> {
    let (encoded, len): (HeartBeat, usize) =
        bincode::decode_from_slice(&buffer.slice(1..), SERDE_CONFIG)
            .map_err(|err| anyhow::anyhow!("Failed to decode heartbeat message: {:?}", err))?;
    Ok((encoded, len + 1))
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

fn parse_file(buffer: Bytes) -> Result<(Bytes, usize)> {
    let (line, mut len) = read_until_crlf_exclusive(&buffer.slice(1..))
        .ok_or(anyhow::anyhow!("Invalid file header"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;
    let content_len: usize = line.parse().context("Invalid file content length")?;

    if len + content_len > buffer.len() {
        return Err(anyhow::anyhow!("Incomplete file data"));
    }

    let file_content = &buffer.slice(len..(len + content_len));

    // Ensure content length is even for hex pairs
    if content_len % 2 != 0 {
        return Err(anyhow::anyhow!("Invalid hex data: odd number of characters"));
    }

    let mut file_bytes = Vec::with_capacity(content_len / 2);
    for chunk in file_content.chunks_exact(2) {
        let hex_str = std::str::from_utf8(chunk).context("Invalid UTF-8 in hex data")?;
        let byte = u8::from_str_radix(hex_str, 16).context("Invalid hex character")?;
        file_bytes.push(byte);
    }

    Ok((Bytes::from(file_bytes), len + content_len))
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

fn serialize_with_bincode<T: bincode::Encode>(prefix: char, arg: &T) -> Bytes {
    // Use size estimation for better performance
    let estimated_size = serialized_len_with_bincode(prefix, arg);
    let mut buffer = BytesMut::with_capacity(estimated_size);

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

impl From<ReplicationAck> for QueryIO {
    fn from(value: ReplicationAck) -> Self {
        QueryIO::Ack(value)
    }
}

impl From<RequestVote> for QueryIO {
    fn from(value: RequestVote) -> Self {
        QueryIO::RequestVote(value)
    }
}

impl From<ElectionVote> for QueryIO {
    fn from(value: ElectionVote) -> Self {
        QueryIO::RequestVoteReply(value)
    }
}

impl From<Topology> for QueryIO {
    fn from(value: Topology) -> Self {
        QueryIO::TopologyChange(value)
    }
}

impl From<()> for QueryIO {
    fn from(_: ()) -> Self {
        QueryIO::Null
    }
}

impl From<HeartBeat> for QueryIO {
    fn from(value: HeartBeat) -> Self {
        QueryIO::ClusterHeartBeat(value)
    }
}
impl From<BatchEntries> for QueryIO {
    fn from(value: BatchEntries) -> Self {
        QueryIO::MigrateBatch(value)
    }
}
impl From<BatchId> for QueryIO {
    fn from(value: BatchId) -> Self {
        QueryIO::MigrationBatchAck(value)
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
    use crate::domains::caches::cache_objects::CacheEntry;
    use crate::domains::cluster_actors::hash_ring::HashRing;
    use crate::domains::replications::ReplicationRole;

    use crate::domains::peers::command::BannedPeer;
    use crate::domains::peers::identifier::PeerIdentifier;
    use crate::domains::replications::LogEntry;
    use crate::domains::replications::state::ReplicationState;

    use uuid::Uuid;

    #[test]
    fn test_deserialize_simple_string() {
        // GIVEN
        let buffer = Bytes::from("+OK\r\n");

        // WHEN
        let (value, len) = parse_simple_string(buffer).unwrap();

        // THEN
        assert_eq!(len, 5);
        assert_eq!(value, Bytes::from("OK"));
    }

    #[test]
    fn test_deserialize_simple_string_ping() {
        // GIVEN
        let buffer = Bytes::from("+PING\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 7);
        assert_eq!(value, QueryIO::SimpleString(Bytes::from("PING")));
    }

    #[test]
    fn test_deserialize_bulk_string() {
        // GIVEN
        let buffer = Bytes::from("$5\r\nhello\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 11);
        assert_eq!(value, QueryIO::BulkString("hello".into()));
    }

    #[test]
    fn test_deserialize_bulk_string_empty() {
        // GIVEN
        let buffer = Bytes::from("$0\r\n\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 6);
        assert_eq!(value, QueryIO::BulkString("".into()));
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
                QueryIO::BulkString("hello".into()),
                QueryIO::BulkString("world".into()),
            ])
        );
    }

    #[test]
    fn test_deserialize_session_request() {
        // GIVEN
        let buffer = Bytes::from("!30\r\n%\x01\0\x05hello\x05world\0");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 21);
        assert_eq!(
            value,
            QueryIO::SessionRequest {
                request_id: 30,
                action: LogEntry::Set {
                    key: "hello".to_string(),
                    value: "world".to_string(),
                    expires_at: None
                }
                .into()
            }
        );
    }
    #[test]
    fn test_serialize_session_request() {
        // GIVEN
        let request = QueryIO::SessionRequest {
            request_id: 30,
            action: LogEntry::Set {
                key: "hello".to_string(),
                value: "world".to_string(),
                expires_at: None,
            }
            .into(),
        };

        // WHEN
        let serialized = request.serialize();

        // THEN
        assert_eq!(serialized, Bytes::from("!30\r\n%\x01\0\x05hello\x05world\0"));
    }

    #[test]
    fn test_parse_file() {
        // GIVEN
        let file = QueryIO::File("hello".into());
        let serialized = file.serialize();
        // WHEN
        let (value, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(value, QueryIO::File("hello".into()));
    }

    #[test]
    fn test_write_operation_to_binary_back_to_itself() {
        // GIVEN
        let op = QueryIO::WriteOperation(WriteOperation {
            entry: LogEntry::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            log_index: 1,
            term: 0,
            session_req: None,
        });

        // WHEN
        let serialized = op.clone().serialize();
        let (deserialized, _) = deserialize(serialized.clone()).unwrap();

        // THEN
        assert_eq!(deserialized, op);
    }

    #[test]
    fn test_acks_to_binary_back_to_acks() {
        // GIVEN
        let follower_res = ReplicationAck { term: 0, rej_reason: None, log_idx: 2 };
        let acks = QueryIO::Ack(follower_res);

        // WHEN
        let serialized = acks.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(deserialized, acks);
    }

    #[test]
    fn test_heartbeat_to_binary_back_to_heartbeat() {
        // GIVEN
        let me = PeerIdentifier::new("127.0.0.1", 6035);
        let leader = ReplicationId::Undecided;
        let banlist = vec![
            BannedPeer { p_id: PeerIdentifier("localhost:28889".into()), ban_time: 3553 },
            BannedPeer { p_id: PeerIdentifier("localhost:22888".into()), ban_time: 3556 },
        ];
        let heartbeat = HeartBeat {
            from: me.clone(),
            term: 1,
            prev_log_index: 0,
            prev_log_term: 1,
            leader_commit_idx: Some(5),
            replid: leader.clone(),
            hop_count: 2,
            banlist,
            append_entries: vec![
                WriteOperation {
                    entry: LogEntry::Set {
                        key: "foo".into(),
                        value: "bar".into(),
                        expires_at: None,
                    },
                    log_index: 1,
                    term: 0,
                    session_req: None,
                },
                WriteOperation {
                    entry: LogEntry::Set {
                        key: "foo".into(),
                        value: "bar".into(),
                        expires_at: Some(323232),
                    },
                    log_index: 2,
                    term: 1,
                    session_req: None,
                },
            ],
            cluster_nodes: vec![
                ReplicationState {
                    node_id: PeerIdentifier("127.0.0.1:30004".into()),
                    last_log_index: 0,
                    replid: ReplicationId::Key(Uuid::now_v7().to_string()),
                    role: ReplicationRole::Follower,
                    term: 1,
                },
                ReplicationState {
                    node_id: PeerIdentifier("127.0.0.1:30002".into()),
                    last_log_index: 0,
                    replid: ReplicationId::Undecided,
                    role: ReplicationRole::Follower,
                    term: 1,
                },
                ReplicationState {
                    node_id: PeerIdentifier("127.0.0.1:30003".into()),
                    last_log_index: 0,
                    replid: ReplicationId::Undecided,
                    role: ReplicationRole::Follower,
                    term: 1,
                },
                ReplicationState {
                    node_id: PeerIdentifier("127.0.0.1:30005".into()),
                    last_log_index: 0,
                    replid: ReplicationId::Key(Uuid::now_v7().to_string()),
                    role: ReplicationRole::Follower,
                    term: 1,
                },
                ReplicationState {
                    node_id: PeerIdentifier("127.0.0.1:30006".into()),
                    last_log_index: 0,
                    replid: ReplicationId::Key(Uuid::now_v7().to_string()),
                    role: ReplicationRole::Follower,
                    term: 1,
                },
                ReplicationState {
                    node_id: PeerIdentifier("127.0.0.1:30001".into()),
                    last_log_index: 0,
                    replid: ReplicationId::Undecided,
                    role: ReplicationRole::Follower,
                    term: 1,
                },
            ],
            hashring: None,
        };
        let replicate = QueryIO::AppendEntriesRPC(heartbeat);

        // WHEN
        let serialized = replicate.clone().serialize();

        let (value, _) = deserialize(serialized).unwrap();

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
        let (deserialized, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(deserialized, request_vote);
    }

    #[test]
    fn test_request_vote_reply_to_binary_back_to_request_vote_reply() {
        // GIVEN
        let request_vote_reply = ElectionVote { term: 1, vote_granted: true };
        let request_vote_reply = QueryIO::RequestVoteReply(request_vote_reply);

        // WHEN
        let serialized = request_vote_reply.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(deserialized, request_vote_reply);
    }

    #[test]
    fn test_topology_change_serde() {
        //GIVEN
        let connected_nodes = vec![
            ReplicationState {
                node_id: PeerIdentifier("localhost:3333".to_string()),
                replid: ReplicationId::Key(Uuid::now_v7().to_string()),
                role: ReplicationRole::Follower,
                last_log_index: 0,
                term: 0,
            },
            ReplicationState {
                node_id: PeerIdentifier("localhost:2222".to_string()),
                replid: ReplicationId::Key(Uuid::now_v7().to_string()),
                role: ReplicationRole::Follower,
                last_log_index: 0,
                term: 0,
            },
        ];
        let hash_ring = HashRing::default();
        let repl_states = connected_nodes
            .iter()
            .map(|peer| ReplicationState {
                node_id: peer.node_id.clone(),
                last_log_index: 0,
                replid: ReplicationId::Key(Uuid::now_v7().to_string()),
                role: ReplicationRole::Follower,
                term: 0,
            })
            .collect();
        let topology = Topology { repl_states, hash_ring };
        let query_io = QueryIO::TopologyChange(topology.clone());

        //WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();
        let QueryIO::TopologyChange(deserialized_topology) = deserialized else {
            panic!("Expected a TopologyChange");
        };
        //THEN
        assert_eq!(deserialized_topology, topology);
    }

    #[test]
    fn test_start_rebalance_serde() {
        //GIVEN
        let query_io = QueryIO::StartRebalance;

        //WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        //THEN
        assert_eq!(deserialized, query_io);
    }

    #[test]
    fn test_heartbeat_include_hashring() {
        // GIVEN
        let ring = HashRing::default()
            .set_partitions(vec![(
                ReplicationId::Key(Uuid::now_v7().to_string()),
                PeerIdentifier::new("127.0.0.1", 3344),
            )])
            .unwrap();

        let heartbeat = HeartBeat {
            from: PeerIdentifier::new("127.0.0.1", 3344),
            term: 1,
            prev_log_index: 0,
            prev_log_term: 1,
            leader_commit_idx: Some(5),
            replid: ReplicationId::Key(Uuid::now_v7().to_string()),
            hop_count: 2,
            banlist: vec![],
            append_entries: vec![],
            cluster_nodes: vec![],
            hashring: Some(Box::new(ring)),
        };

        let query_io = QueryIO::ClusterHeartBeat(heartbeat.clone());

        // WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();
        let QueryIO::ClusterHeartBeat(deserialized_heartbeat) = deserialized else {
            panic!("Expected a ClusterHeartBeat");
        };

        // THEN -- ring should be included in the heartbeat
        assert_eq!(deserialized_heartbeat.hashring, heartbeat.hashring);
        let ring_to_cmp = heartbeat.hashring.unwrap();
        let deserialized_ring = deserialized_heartbeat.hashring.unwrap();
        assert_eq!(deserialized_ring.get_pnode_count(), ring_to_cmp.get_pnode_count());

        assert_eq!(deserialized_ring, ring_to_cmp);
        assert!(!ring_to_cmp.get_virtual_nodes().is_empty());
        assert!(!deserialized_ring.get_virtual_nodes().is_empty());
    }

    #[test]
    fn test_migrate_batch_serde() {
        // GIVEN
        let migrate_batch = BatchEntries {
            batch_id: BatchId(Uuid::now_v7().to_string()),
            entries: vec![CacheEntry::new("foo", "bar")],
        };
        let query_io = QueryIO::MigrateBatch(migrate_batch.clone());

        // WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(deserialized, query_io);
        let QueryIO::MigrateBatch(deserialized_migrate_batch) = deserialized else {
            panic!("Expected a MigrateBatch");
        };
        assert_eq!(deserialized_migrate_batch.batch_id, migrate_batch.batch_id);
        assert_eq!(deserialized_migrate_batch.entries, migrate_batch.entries);
    }

    #[test]
    fn test_migration_batch_ack_serde() {
        // GIVEN
        let migration_batch_ack = BatchId(Uuid::now_v7().to_string());
        let query_io = QueryIO::MigrationBatchAck(migration_batch_ack.clone());

        // WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(deserialized, query_io);
        let QueryIO::MigrationBatchAck(deserialized_migration_batch_ack) = deserialized else {
            panic!("Expected a MigrationBatchAck");
        };
        assert_eq!(deserialized_migration_batch_ack, migration_batch_ack);
    }

    #[test]
    fn test_close_connection_serde() {
        //GIVEN
        let query_io = QueryIO::CloseConnection;

        // WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        //THEn
        assert_eq!(deserialized, query_io);
        let QueryIO::CloseConnection = deserialized else { panic!("Expected CloseConnection") };
    }

    #[test]
    fn test_merge_arrays() {
        // GIVEN
        let array1 =
            QueryIO::Array(vec![QueryIO::BulkString("a".into()), QueryIO::BulkString("b".into())]);
        let array2 =
            QueryIO::Array(vec![QueryIO::BulkString("c".into()), QueryIO::BulkString("d".into())]);

        // WHEN
        let merged = array1.merge(array2).unwrap();

        // THEN
        assert_eq!(
            merged,
            QueryIO::Array(vec![
                QueryIO::BulkString("a".into()),
                QueryIO::BulkString("b".into()),
                QueryIO::BulkString("c".into()),
                QueryIO::BulkString("d".into())
            ])
        );
    }

    #[test]
    fn test_merge_array_with_single_element() {
        // GIVEN
        let array = QueryIO::Array(vec![QueryIO::BulkString("a".into())]);
        let single_element = QueryIO::BulkString("b".into());

        // WHEN
        let merged = array.merge(single_element).unwrap();

        // THEN
        assert_eq!(
            merged,
            QueryIO::Array(vec![QueryIO::BulkString("a".into()), QueryIO::BulkString("b".into())])
        );
    }

    #[test]
    fn test_merge_null_with_array() {
        // GIVEN
        let array = QueryIO::Array(vec![QueryIO::BulkString("a".into())]);
        let null_value = QueryIO::Null;

        // WHEN
        let merged = array.clone().merge(null_value).unwrap();

        // THEN
        assert_eq!(merged, array);
    }
}
