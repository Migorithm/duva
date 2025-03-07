use crate::{
    domains::query_parsers::{QueryIO, deserialize},
    from_to, make_smart_pointer, write_array,
};
use bytes::{Bytes, BytesMut};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOperation {
    pub request: WriteRequest,
    pub log_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy, Hash, PartialOrd, Ord)]
pub struct LogIndex(pub(crate) u64);
make_smart_pointer!(LogIndex, u64);
from_to!(u64, LogIndex);
impl std::fmt::Display for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for LogIndex {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(LogIndex(s.parse()?))
    }
}

/// Operations that appear in the Append-Only File (WAL).
/// Client request is converted to WriteOperation and then it turns into WriteOp when it gets offset
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRequest {
    Set { key: String, value: String },
    SetWithExpiry { key: String, value: String, expires_at: u64 },
    Delete { key: String },
}

impl WriteOperation {
    pub fn serialize(self) -> Bytes {
        QueryIO::ReplicateLog(self).serialize()
    }
}

impl WriteRequest {
    pub(crate) fn key(&self) -> String {
        match self {
            WriteRequest::Set { key, .. } => key.clone(),
            WriteRequest::SetWithExpiry { key, .. } => key.clone(),
            WriteRequest::Delete { key } => key.clone(),
        }
    }
    pub fn to_array(self) -> QueryIO {
        match self {
            WriteRequest::Set { key, value } => write_array!("SET", key, value),
            WriteRequest::SetWithExpiry { key, value, expires_at } => {
                write_array!("SET", key, value, "px", expires_at.to_string())
            },
            WriteRequest::Delete { key } => write_array!("DEL", key),
        }
    }

    /// Deserialize `WriteOperation`s from the given bytes.
    pub fn deserialize(mut bytes: BytesMut) -> anyhow::Result<Vec<WriteOperation>> {
        let mut ops: Vec<WriteOperation> = Vec::new();

        while !bytes.is_empty() {
            let (query, consumed) = deserialize(bytes.clone())?;
            bytes = bytes.split_off(consumed);

            let QueryIO::ReplicateLog(write_operation) = query else {
                return Err(anyhow::anyhow!("expected replicate"));
            };
            ops.push(write_operation);
        }

        Ok(ops)
    }

    pub fn new(cmd: String, args: std::vec::IntoIter<QueryIO>) -> anyhow::Result<Self> {
        match cmd.as_str() {
            "set" => Self::to_set(args),

            _ => Err(anyhow::anyhow!("unsupported command")),
        }
    }

    pub fn to_set(mut args: std::vec::IntoIter<QueryIO>) -> anyhow::Result<Self> {
        match args.len() {
            2 => {
                let (Some(QueryIO::BulkString(key)), Some(QueryIO::BulkString(value))) =
                    (args.next(), args.next())
                else {
                    return Err(anyhow::anyhow!("expected value"));
                };

                Ok(WriteRequest::Set {
                    key: String::from_utf8(key.to_vec())?,
                    value: String::from_utf8(value.to_vec())?,
                })
            },

            4 => {
                let (
                    Some(QueryIO::BulkString(key)),
                    Some(QueryIO::BulkString(value)),
                    Some(QueryIO::BulkString(_)),
                    Some(QueryIO::BulkString(expiry)),
                ) = (args.next(), args.next(), args.next(), args.next())
                else {
                    return Err(anyhow::anyhow!("expected value"));
                };

                Ok(WriteRequest::SetWithExpiry {
                    key: String::from_utf8(key.to_vec())?,
                    value: String::from_utf8(value.to_vec())?,
                    expires_at: std::str::from_utf8(&expiry)?.parse()?,
                })
            },

            _ => Err(anyhow::anyhow!("expected 2 or 4 arguments")),
        }
    }
}

impl From<WriteOperation> for Bytes {
    fn from(op: WriteOperation) -> Self {
        op.serialize()
    }
}
