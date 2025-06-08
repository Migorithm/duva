use crate::domains::caches::cache_objects::CacheValue;
use crate::domains::operation_logs::WriteOperation;
use crate::domains::peers::command::{
    ElectionVote, HeartBeat, MigrateBatch, MigrationBatchAck, ReplicationAck, RequestVote,
};
use crate::prelude::PeerIdentifier;
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
const TOPOLOGY_CHANGE_PREFIX: char = 't';
const START_REBALANCE_PREFIX: char = 'T';
const REPLICATE_PREFIX: char = '#';
const ACKS_PREFIX: char = '@';
const REQUEST_VOTE_PREFIX: char = 'v';
const REQUEST_VOTE_REPLY_PREFIX: char = 'r';
const SESSION_REQUEST_PREFIX: char = '!';
const MIGRATE_BATCH_PREFIX: char = 'm';
const MIGRATION_BATCH_ACK_PREFIX: char = 'M';

const ERR_PREFIX: char = '-';
const NULL_PREFIX: char = '\u{0000}';
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
    SimpleString(String),
    BulkString(String),
    Array(Vec<QueryIO>),
    SessionRequest {
        request_id: u64,
        value: Vec<QueryIO>,
    },
    Err(String),

    // custom types
    File(Bytes),
    AppendEntriesRPC(HeartBeat),
    ClusterHeartBeat(HeartBeat),
    WriteOperation(WriteOperation),
    Ack(ReplicationAck),
    RequestVote(RequestVote),
    RequestVoteReply(ElectionVote),

    TopologyChange(Vec<PeerIdentifier>),
    StartRebalance,
    MigrateBatch(MigrateBatch),
    MigrationBatchAck(MigrationBatchAck),
}

impl QueryIO {
    pub fn serialize(self) -> Bytes {
        match self {
            | QueryIO::Null => NULL_PREFIX.to_string().into(),
            | QueryIO::SimpleString(s) => {
                let mut buffer =
                    String::with_capacity(SIMPLE_STRING_PREFIX.len_utf8() + s.len() + 2);
                write!(&mut buffer, "{}{}\r\n", SIMPLE_STRING_PREFIX, s).unwrap();
                buffer.into()
            },
            | QueryIO::BulkString(s) => {
                let mut byte_mut = BytesMut::with_capacity(1 + 1 + s.len() + 4);
                byte_mut.extend_from_slice(BULK_STRING_PREFIX.encode_utf8(&mut [0; 4]).as_bytes());
                byte_mut.extend_from_slice(s.len().to_string().as_bytes());
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.extend_from_slice(s.as_bytes());
                byte_mut.extend_from_slice(b"\r\n");
                byte_mut.freeze()
            },
            | QueryIO::File(f) => {
                let file_len = f.len() * 2;
                let mut hex_file = String::with_capacity(file_len + file_len.to_string().len() + 2);

                // * To avoid the overhead of using format! macro by creating intermediate string, use write!
                let _ = write!(&mut hex_file, "{}{}\r\n", FILE_PREFIX, file_len);
                f.into_iter().for_each(|byte| {
                    let _ = write!(hex_file, "{:02x}", byte);
                });

                hex_file.into()
            },
            | QueryIO::Array(array) => {
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
            | QueryIO::SessionRequest { request_id, value } => {
                let mut buffer = BytesMut::with_capacity(32 + 1 + value.len() * 32);
                buffer.extend_from_slice(format!("!{}\r\n", request_id).as_bytes());
                buffer.extend_from_slice(&QueryIO::Array(value).serialize());
                buffer.freeze()
            },
            | QueryIO::Err(e) => Bytes::from([ERR_PREFIX.to_string(), e, "\r\n".into()].concat()),
            | QueryIO::AppendEntriesRPC(heartbeat) => {
                serialize_with_bincode(APPEND_ENTRY_RPC_PREFIX, &heartbeat)
            },
            | QueryIO::WriteOperation(write_operation) => {
                serialize_with_bincode(REPLICATE_PREFIX, &write_operation)
            },
            | QueryIO::Ack(items) => serialize_with_bincode(ACKS_PREFIX, &items),
            | QueryIO::RequestVote(request_vote) => {
                serialize_with_bincode(REQUEST_VOTE_PREFIX, &request_vote)
            },
            | QueryIO::RequestVoteReply(request_vote_reply) => {
                serialize_with_bincode(REQUEST_VOTE_REPLY_PREFIX, &request_vote_reply)
            },
            | QueryIO::ClusterHeartBeat(heart_beat_message) => {
                serialize_with_bincode(CLUSTER_HEARTBEAT_PREFIX, &heart_beat_message)
            },
            | QueryIO::TopologyChange(peer_identifiers) => {
                serialize_with_bincode(TOPOLOGY_CHANGE_PREFIX, &peer_identifiers)
            },
            | QueryIO::StartRebalance => serialize_with_bincode(START_REBALANCE_PREFIX, &()),
            | QueryIO::MigrateBatch(migrate_batch) => {
                serialize_with_bincode(MIGRATE_BATCH_PREFIX, &migrate_batch)
            },
            | QueryIO::MigrationBatchAck(migration_batch_ack) => {
                serialize_with_bincode(MIGRATION_BATCH_ACK_PREFIX, &migration_batch_ack)
            },
        }
    }

    pub fn unpack_single_entry<T>(self) -> Result<T>
    where
        T: std::str::FromStr<Err: std::error::Error + Sync + Send + 'static>,
    {
        match self {
            | QueryIO::BulkString(s) => Ok(String::from_utf8(s.into())?.parse::<T>()?),

            | _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
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
        QueryIO::BulkString(value)
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
            | Some(cache_value) => QueryIO::BulkString(cache_value.value),
            | None => QueryIO::Null,
        }
    }
}
impl From<Option<String>> for QueryIO {
    fn from(v: Option<String>) -> Self {
        match v {
            | Some(v) => QueryIO::BulkString(v),
            | None => QueryIO::Null,
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
    match buffer[0] as char {
        | SIMPLE_STRING_PREFIX => {
            let (bytes, len) = parse_simple_string(buffer)?;
            Ok((QueryIO::SimpleString(bytes), len))
        },
        | ARRAY_PREFIX => parse_array(buffer),
        | SESSION_REQUEST_PREFIX => parse_session_request(buffer),
        | BULK_STRING_PREFIX => {
            let (bytes, len) = parse_bulk_string(buffer)?;
            Ok((QueryIO::BulkString(bytes), len))
        },
        | FILE_PREFIX => {
            let (bytes, len) = parse_file(buffer)?;
            Ok((QueryIO::File(bytes), len))
        },
        | ERR_PREFIX => {
            let (bytes, len) = parse_simple_string(buffer)?;
            Ok((QueryIO::Err(bytes), len))
        },
        | NULL_PREFIX => Ok((QueryIO::Null, 1)),

        | APPEND_ENTRY_RPC_PREFIX => {
            let (heartbeat, len) = parse_heartbeat(buffer)?;
            Ok((QueryIO::AppendEntriesRPC(heartbeat), len))
        },
        | CLUSTER_HEARTBEAT_PREFIX => {
            let (heartbeat, len) = parse_heartbeat(buffer)?;
            Ok((QueryIO::ClusterHeartBeat(heartbeat), len))
        },
        | REPLICATE_PREFIX => parse_custom_type::<WriteOperation>(buffer),
        | ACKS_PREFIX => parse_custom_type::<ReplicationAck>(buffer),
        | REQUEST_VOTE_PREFIX => parse_custom_type::<RequestVote>(buffer),
        | REQUEST_VOTE_REPLY_PREFIX => parse_custom_type::<ElectionVote>(buffer),
        | TOPOLOGY_CHANGE_PREFIX => parse_custom_type::<Vec<PeerIdentifier>>(buffer),
        | START_REBALANCE_PREFIX => Ok((QueryIO::StartRebalance, 1)),
        | MIGRATE_BATCH_PREFIX => parse_custom_type::<MigrateBatch>(buffer),
        | MIGRATION_BATCH_ACK_PREFIX => parse_custom_type::<MigrationBatchAck>(buffer),
        | _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

// +PING\r\n
pub(crate) fn parse_simple_string(buffer: Bytes) -> Result<(String, usize)> {
    let (line, len) = read_until_crlf_exclusive(&buffer.slice(1..))
        .ok_or(anyhow::anyhow!("Invalid simple string"))?;
    Ok((line, len + 1))
}

fn parse_array(buffer: Bytes) -> Result<(QueryIO, usize)> {
    let mut offset = 0;
    offset += 1;

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
    let mut offset = 0;
    // ! to advance '!'
    offset += 1;

    let (count_bytes, count_len) = read_until_crlf_exclusive(&buffer.slice(offset..))
        .ok_or(anyhow::anyhow!("Invalid array length"))?;
    offset += count_len;
    let request_id = count_bytes.parse()?;

    // ! to advance '$'
    offset += 1;

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
    Ok((QueryIO::SessionRequest { request_id, value: elements }, offset))
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

fn parse_bulk_string(buffer: Bytes) -> Result<(String, usize)> {
    let (line, mut len) = read_until_crlf_exclusive(&buffer.slice(1..))
        .ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;

    let content_len: usize = line.parse()?;
    let (line, total_len) = read_content_until_crlf(&buffer.slice(len..), content_len)
        .context("Invalid BulkString format!")?;
    Ok((line, len + total_len))
}

fn parse_file(buffer: Bytes) -> Result<(Bytes, usize)> {
    let (line, mut len) = read_until_crlf_exclusive(&buffer.slice(1..))
        .ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;
    let content_len: usize = line.parse()?;

    let file_content = &buffer.slice(len..(len + content_len));

    let file = file_content
        .chunks(2)
        .flat_map(|chunk| std::str::from_utf8(chunk).map(|s| u8::from_str_radix(s, 16)))
        .collect::<Result<Bytes, _>>()?;

    Ok((file, len + content_len))
}
pub(super) fn read_content_until_crlf(
    buffer: &Bytes,
    content_len: usize,
) -> Option<(String, usize)> {
    if buffer.len() < content_len + 2 {
        return None;
    }
    if buffer[content_len] == b'\r' && buffer[content_len + 1] == b'\n' {
        return Some((
            String::from_utf8_lossy(&buffer.slice(0..content_len)).to_string(),
            content_len + 2,
        ));
    }
    None
}

/// None if crlf not found.
#[inline]
pub(super) fn read_until_crlf_exclusive(buffer: &Bytes) -> Option<(String, usize)> {
    memchr::memmem::find(buffer, b"\r\n")
        .map(|i| (String::from_utf8_lossy(&buffer.slice(0..i)).to_string(), i + 2))
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

impl From<Vec<PeerIdentifier>> for QueryIO {
    fn from(value: Vec<PeerIdentifier>) -> Self {
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
impl From<MigrateBatch> for QueryIO {
    fn from(value: MigrateBatch) -> Self {
        QueryIO::MigrateBatch(value)
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::domains::caches::cache_objects::CacheEntry;
    use crate::domains::cluster_actors::hash_ring::{BatchId, HashRing};
    use crate::domains::cluster_actors::replication::ReplicationId;

    use crate::domains::operation_logs::WriteRequest;
    use crate::domains::peers::command::{BannedPeer, RejectionReason};
    use crate::domains::peers::identifier::PeerIdentifier;
    use crate::domains::peers::peer::{NodeKind, PeerState};

    use super::*;

    #[test]
    fn test_deserialize_simple_string() {
        // GIVEN
        let buffer = Bytes::from("+OK\r\n");

        // WHEN
        let (value, len) = parse_simple_string(buffer).unwrap();

        // THEN
        assert_eq!(len, 5);
        assert_eq!(value, "OK".to_string());
    }

    #[test]
    fn test_deserialize_simple_string_ping() {
        // GIVEN
        let buffer = Bytes::from("+PING\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 7);
        assert_eq!(value, QueryIO::SimpleString("PING".to_string().into()));
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
        let buffer = Bytes::from("!30\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        // WHEN
        let (value, len) = deserialize(buffer).unwrap();

        // THEN
        assert_eq!(len, 31);
        assert_eq!(
            value,
            QueryIO::SessionRequest {
                request_id: 30,
                value: vec![
                    QueryIO::BulkString("hello".into()),
                    QueryIO::BulkString("world".into()),
                ]
            }
        );
    }
    #[test]
    fn test_serialize_session_request() {
        // GIVEN
        let request = QueryIO::SessionRequest {
            request_id: 30,
            value: vec![QueryIO::BulkString("hello".into()), QueryIO::BulkString("world".into())],
        };

        // WHEN
        let serialized = request.serialize();

        // THEN
        assert_eq!(serialized, Bytes::from("!30\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"));
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
            request: WriteRequest::Set { key: "foo".into(), value: "bar".into(), expires_at: None },
            log_index: 1,
            term: 0,
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
        let follower_res = ReplicationAck {
            term: 0,
            rej_reason: RejectionReason::None,
            log_idx: 2,
            from: PeerIdentifier("repl1".into()),
        };
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
        let me = PeerIdentifier::new("me".into(), 6035);
        let leader = ReplicationId::Undecided;
        let banned_list = vec![
            BannedPeer { p_id: PeerIdentifier("banned1".into()), ban_time: 3553 },
            BannedPeer { p_id: PeerIdentifier("banned2".into()), ban_time: 3556 },
        ];
        let heartbeat = HeartBeat {
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
                    request: WriteRequest::Set {
                        key: "foo".into(),
                        value: "bar".into(),
                        expires_at: None,
                    },
                    log_index: 1,
                    term: 0,
                },
                WriteOperation {
                    request: WriteRequest::Set {
                        key: "foo".into(),
                        value: "bar".into(),
                        expires_at: Some(323232),
                    },
                    log_index: 2,
                    term: 1,
                },
            ],
            cluster_nodes: vec![
                PeerState::new(
                    "127.0.0.1:30004",
                    0,
                    ReplicationId::Key(Uuid::now_v7().to_string()),
                    NodeKind::Replica,
                ),
                PeerState::new("127.0.0.1:30002", 0, ReplicationId::Undecided, NodeKind::Replica),
                PeerState::new("127.0.0.1:30003", 0, ReplicationId::Undecided, NodeKind::Replica),
                PeerState::new(
                    "127.0.0.1:30005",
                    0,
                    ReplicationId::Key(Uuid::now_v7().to_string()),
                    NodeKind::Replica,
                ),
                PeerState::new(
                    "127.0.0.1:30006",
                    0,
                    ReplicationId::Key(Uuid::now_v7().to_string()),
                    NodeKind::Replica,
                ),
                PeerState::new("127.0.0.1:30001", 0, ReplicationId::Undecided, NodeKind::Myself),
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
        let topology =
            vec!["127.0.0.1:6000".to_string().into(), "127.0.0.1:6001".to_string().into()];
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
            .add_partition_if_not_exists(
                ReplicationId::Key(Uuid::now_v7().to_string()),
                PeerIdentifier::new("127.0.1:3344".into(), 0),
            )
            .unwrap();

        let heartbeat = HeartBeat {
            from: PeerIdentifier::new("127.0.0.1:3344".into(), 0),
            term: 1,
            prev_log_index: 0,
            prev_log_term: 1,
            hwm: 5,
            replid: ReplicationId::Key(Uuid::now_v7().to_string()),
            hop_count: 2,
            ban_list: vec![],
            append_entries: vec![],
            cluster_nodes: vec![],
            hashring: Some(ring),
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
        assert!(ring_to_cmp.get_virtual_nodes().len() > 0);
        assert!(deserialized_ring.get_virtual_nodes().len() > 0);
    }

    #[test]
    fn test_migrate_batch_serde() {
        // GIVEN
        let migrate_batch = MigrateBatch {
            batch_id: BatchId(Uuid::now_v7().to_string()),
            cache_entries: vec![CacheEntry::new("foo".into(), CacheValue::new("bar".into()))],
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
        assert_eq!(deserialized_migrate_batch.cache_entries, migrate_batch.cache_entries);
    }

    #[test]
    fn test_migration_batch_ack_serde() {
        // GIVEN
        let migration_batch_ack =
            MigrationBatchAck { batch_id: BatchId(Uuid::now_v7().to_string()), success: true };
        let query_io = QueryIO::MigrationBatchAck(migration_batch_ack.clone());

        // WHEN
        let serialized = query_io.clone().serialize();
        let (deserialized, _) = deserialize(serialized).unwrap();

        // THEN
        assert_eq!(deserialized, query_io);
        let QueryIO::MigrationBatchAck(deserialized_migration_batch_ack) = deserialized else {
            panic!("Expected a MigrationBatchAck");
        };
        assert_eq!(deserialized_migration_batch_ack.batch_id, migration_batch_ack.batch_id);
        assert_eq!(deserialized_migration_batch_ack.success, migration_batch_ack.success);
    }
}
