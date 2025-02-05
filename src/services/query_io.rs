use std::fmt::Write;

use crate::services::cluster::replications::replication::{time_in_secs, PeerState};
use crate::services::statefuls::cache::CacheValue;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};

// ! CURRENTLY, only ascii unicode(0-127) is supported
const FILE_PREFIX: char = '\u{0066}';
const SIMPLE_STRING_PREFIX: char = '+';
const BULK_STRING_PREFIX: char = '$';
const ARRAY_PREFIX: char = '*';
const ERROR_PREFIX: char = '-';
const PEERSTATE_PREFIX: char = '^';

#[macro_export]
macro_rules! write_array {
    ($($x:expr),*) => {
        $crate::services::query_io::QueryIO::Array(vec![$($crate::services::query_io::QueryIO::BulkString($x.into())),*])
    };
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryIO {
    SimpleString(Bytes),
    BulkString(Bytes),
    Array(Vec<QueryIO>),
    Null,
    Err(Bytes),
    File(Bytes),
    PeerState(PeerState),
}

impl QueryIO {
    pub fn serialize(self) -> Bytes {
        let concatenator = |prefix: char| -> Bytes { Bytes::from_iter([prefix as u8]) };

        match self {
            QueryIO::SimpleString(s) => {
                Bytes::from([concatenator(SIMPLE_STRING_PREFIX), s, "\r\n".into()].concat())
            }
            QueryIO::BulkString(s) => Bytes::from(
                [
                    concatenator(BULK_STRING_PREFIX),
                    s.len().to_string().into_bytes().into(),
                    "\r\n".into(),
                    s,
                    "\r\n".into(),
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
            }
            QueryIO::Err(e) => Bytes::from([concatenator(ERROR_PREFIX), e, "\r\n".into()].concat()),

            QueryIO::Null => "$-1\r\n".into(),

            QueryIO::Array(array) => [
                Into::<Bytes>::into(format!("{}{}\r\n", ARRAY_PREFIX, array.len()).into_bytes()),
                array.into_iter().flat_map(|v| v.serialize()).collect(),
            ]
            .concat()
            .into(),
            QueryIO::PeerState(PeerState {
                term,
                offset,
                master_replid,
                hop_count,
                heartbeat_from: id,
                ban_list,
            }) => {
                let message = format!(
                            "{}\r\n${}\r\n{term}\r\n${}\r\n{offset}\r\n${}\r\n{master_replid}\r\n${}\r\n{hop_count}\r\n${}\r\n{id}\r\n{ARRAY_PREFIX}{}\r\n",
                            PEERSTATE_PREFIX,
                            term.to_string().len(),
                            offset.to_string().len(),
                            master_replid.len(),
                            hop_count.to_string().len(),
                            id.len(),
                            ban_list.len()
                            ).into();

                let long_bytes = Bytes::from(
                    ban_list
                        .into_iter()
                        .flat_map(|x| QueryIO::BulkString(x.into()).serialize()) // Convert to Vec<u8>
                        .collect::<Vec<u8>>(),
                );

                [message, long_bytes].concat().into()
            }
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

impl From<Option<CacheValue>> for QueryIO {
    fn from(v: Option<CacheValue>) -> Self {
        match v {
            Some(CacheValue::Value(v)) => QueryIO::BulkString(v.into()),
            Some(CacheValue::ValueWithExpiry(v, _exp)) => QueryIO::BulkString(v.into()),
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
        let QueryIO::PeerState(state) = value else {
            return Err(anyhow::anyhow!("invalid QueryIO invariant"));
        };

        Ok(state)
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
        }
        ARRAY_PREFIX => parse_array(buffer),
        BULK_STRING_PREFIX => {
            let (bytes, len) = parse_bulk_string(buffer)?;
            Ok((QueryIO::BulkString(bytes), len))
        }
        FILE_PREFIX => {
            let (bytes, len) = parse_file(buffer)?;
            Ok((QueryIO::File(bytes), len))
        }
        PEERSTATE_PREFIX => parse_peer_state(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

// +PING\r\n
pub(crate) fn parse_simple_string(buffer: BytesMut) -> Result<(Bytes, usize)> {
    let (line, len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid simple string"))?;
    Ok((line, len + 1))
}

fn parse_array(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    len += 1;

    let array_len_in_str = String::from_utf8(line.to_vec())?;
    let len_of_array = array_len_in_str.parse()?;

    let mut bulk_strings = Vec::with_capacity(len_of_array);

    for _ in 0..len_of_array {
        let (value, l) = deserialize(BytesMut::from(&buffer[len..]))?;
        bulk_strings.push(value);
        len += l;
    }

    Ok((QueryIO::Array(bulk_strings), len))
}

fn parse_peer_state(buffer: BytesMut) -> Result<(QueryIO, usize)> {
    // fixed rule for peer state
    let len = 3;

    let (term, l1) = deserialize(BytesMut::from(&buffer[len..]))?;
    let (offset, l2) = deserialize(BytesMut::from(&buffer[len + l1..]))?;
    let (master_replid, l3) = deserialize(BytesMut::from(&buffer[len + l1 + l2..]))?;
    let (hop_count, l4) = deserialize(BytesMut::from(&buffer[len + l1 + l2 + l3..]))?;
    let (id, l5) = deserialize(BytesMut::from(&buffer[len + l1 + l2 + l3 + l4..]))?;
    let (ban_list, l6) = deserialize(BytesMut::from(&buffer[len + l1 + l2 + l3 + l4 + l5..]))?;

    Ok((
        QueryIO::PeerState(PeerState {
            heartbeat_from: id.unpack_single_entry()?,
            term: term.unpack_single_entry()?,
            offset: offset.unpack_single_entry()?,
            master_replid: master_replid.unpack_single_entry()?,
            hop_count: hop_count.unpack_single_entry()?,
            ban_list: ban_list.unpack_array()?,
        }),
        len + l1 + l2 + l3 + l4 + l5 + l6,
    ))
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Bytes, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;

    let content_len: usize = String::from_utf8(line.to_vec())?.parse()?;

    let (line, _) = read_until_crlf(&buffer[len..].into()).context("Invalid BulkString format!")?;

    Ok((line, len + content_len + 2))
}

fn parse_file(buffer: BytesMut) -> Result<(Bytes, usize)> {
    let (line, mut len) =
        read_until_crlf(&buffer[1..].into()).ok_or(anyhow::anyhow!("Invalid bulk string"))?;

    // Adjust `len` to include the initial line and calculate `bulk_str_len`
    len += 1;
    let content_len: usize = String::from_utf8(line.to_vec())?.parse()?;

    let file_content = &buffer[len..(len + content_len)];

    let file = file_content
        .chunks(2)
        .flat_map(|chunk| std::str::from_utf8(chunk).map(|s| u8::from_str_radix(s, 16)))
        .collect::<Result<Bytes, _>>()?;

    Ok((file, len + content_len))
}

fn read_until_crlf(buffer: &BytesMut) -> Option<(Bytes, usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((Bytes::copy_from_slice(&buffer[0..(i - 1)]), i + 1));
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
    assert_eq!(value, b"OK".to_vec());
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
    let data = "^\r\n$3\r\n245\r\n$7\r\n1234329\r\n$4\r\nabcd\r\n$1\r\n2\r\n$15\r\n127.0.0.1:49153\r\n*0\r\n";

    let buffer = BytesMut::from(data);

    // WHEN
    let (value, len) = deserialize(buffer).unwrap();

    // THEN
    assert_eq!(len, data.len());
    assert_eq!(
        value,
        QueryIO::PeerState(PeerState {
            term: 245,
            offset: 1234329,
            master_replid: "abcd".into(),
            hop_count: 2,
            heartbeat_from: "127.0.0.1:49153".to_string().into(),
            ban_list: vec![]
        })
    );
    let peer_state: PeerState = value.try_into().unwrap();
    assert_eq!(peer_state.term, 245);
    assert_eq!(peer_state.offset, 1234329);

    assert_eq!(peer_state.master_replid, "abcd");
    assert_eq!(peer_state.hop_count, 2);
    assert!(peer_state.ban_list.is_empty())
}

#[test]
fn test_from_peer_state_to_bytes() {
    use crate::services::query_io::QueryIO;

    //GIVEN

    let peer_state = PeerState {
        term: 1,
        offset: 2,
        master_replid: "your_master_repl".into(),
        hop_count: 2,
        heartbeat_from: "127.0.0.1:49152".to_string().into(),
        ban_list: Default::default(),
    };
    //WHEN
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();
    //THEN
    assert_eq!(
        "^\r\n$1\r\n1\r\n$1\r\n2\r\n$16\r\nyour_master_repl\r\n$1\r\n2\r\n$15\r\n127.0.0.1:49152\r\n*0\r\n",
        peer_state_serialized
    );

    //GIVEN
    let peer_state = PeerState {
        term: 5,
        offset: 3232,
        master_replid: "your_master_repl2".into(),
        hop_count: 40,
        heartbeat_from: "127.0.0.1:49159".to_string().into(),
        ban_list: Default::default(),
    };
    //WHEN
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();
    //THEN
    assert_eq!(
        "^\r\n$1\r\n5\r\n$4\r\n3232\r\n$17\r\nyour_master_repl2\r\n$2\r\n40\r\n$15\r\n127.0.0.1:49159\r\n*0\r\n",
        peer_state_serialized
    );
}

#[test]
fn test_peer_state_ban_list_to_binary() {
    use crate::services::cluster::peers::identifier::PeerIdentifier;
    use crate::services::cluster::replications::replication::Replication;
    // GIVEN
    let mut replication = Replication::default();
    let peer_id = PeerIdentifier::new("127.0.0.1", 6739);
    replication.ban_peer(&peer_id).unwrap();

    let ban_time = replication.ban_list[0].ban_time;

    //WHEN
    let peer_state = replication.current_state(1);
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();

    //THEN
    let expected = format!("^\r\n$1\r\n0\r\n$1\r\n0\r\n$40\r\n{}\r\n$1\r\n1\r\n$14\r\n127.0.0.1:6379\r\n*1\r\n$25\r\n127.0.0.1:6739-{}\r\n",replication.master_replid,ban_time);
    assert_eq!(expected, peer_state_serialized);
}

#[test]
fn test_binary_to_banned_list() {
    use crate::services::cluster::peers::identifier::PeerIdentifier;
    use crate::services::cluster::replications::replication::BannedPeer;

    // GIVEN
    let binary= format!("^\r\n$1\r\n0\r\n$1\r\n0\r\n$6\r\n{}\r\n$1\r\n1\r\n$14\r\n127.0.0.1:6379\r\n*1\r\n$25\r\n127.0.0.1:6739-{}\r\n","random",6545442);
    let buffer = BytesMut::from_iter(binary.into_bytes());

    // WHEN
    let (value, _) = deserialize(buffer).unwrap();

    // THEN
    assert_eq!(
        value,
        QueryIO::PeerState(PeerState {
            term: 0,
            offset: 0,
            master_replid: "random".into(),
            hop_count: 1,
            heartbeat_from: "127.0.0.1:6379".to_string().into(),
            ban_list: [BannedPeer {
                p_id: PeerIdentifier("127.0.0.1:6739".into()),
                ban_time: 6545442
            }]
            .to_vec()
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
    assert_eq!(len, 15);
    assert_eq!(value, QueryIO::File("hello".into()));
}

#[test]
fn test_banned_peer_serde() {
    use crate::services::cluster::peers::identifier::PeerIdentifier;
    use crate::services::cluster::replications::replication::BannedPeer;
    use crate::services::cluster::replications::replication::Replication;

    //GIVEN
    let mut replication = Replication::default();
    let peer_id = PeerIdentifier::new("127.0.0.1", 6739);

    //WHEN
    replication.ban_peer(&peer_id).unwrap();
    let banned_peer_in_bytes: Bytes = replication.ban_list[0].clone().try_into().unwrap();

    let banned_peer: BannedPeer =
        String::from_utf8(banned_peer_in_bytes.to_vec()).unwrap().parse().unwrap();
    // less than 1 second passed

    let current = time_in_secs().unwrap();

    //THEN
    assert_eq!(banned_peer.ban_time, current);
}

#[test]
fn test_banned_peer_serde_when_time_passed() {
    use crate::services::cluster::peers::identifier::PeerIdentifier;
    use crate::services::cluster::replications::replication::BannedPeer;
    use crate::services::cluster::replications::replication::Replication;

    //GIVEN
    let mut replication = Replication::default();
    let peer_id = PeerIdentifier::new("127.0.0.1", 6739);

    //WHEN
    replication.ban_peer(&peer_id).unwrap();
    let banned_peer_in_bytes: Bytes = replication.ban_list[0].clone().try_into().unwrap();

    let banned_peer: BannedPeer =
        String::from_utf8(banned_peer_in_bytes.to_vec()).unwrap().parse().unwrap();
    // less than 1 second passed

    std::thread::sleep(std::time::Duration::from_secs(1));

    let current = time_in_secs().unwrap();

    //THEN
    assert_ne!(banned_peer.ban_time, current);
}
