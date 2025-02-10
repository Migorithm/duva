use anyhow::Result;
use bytes::{Bytes, BytesMut};

use crate::write_array;

use super::{
    client::request::ClientRequest,
    query_io::{deserialize as deserialize_query_io, QueryIO},
};

/// Trait for an Append-Only File (AOF) abstraction.
pub trait TAof {
    /// Appends a single `WriteOperation` to the log.
    fn append(
        &mut self,
        op: WriteOperation,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Replays all logged operations from the beginning of the AOF, calling the provided callback `f` for each operation.
    ///
    /// The callback `f(WriteOperation)` receives each operation in the order it was appended.
    fn replay<F>(&mut self, f: F) -> impl std::future::Future<Output = Result<()>> + Send
    where
        F: FnMut(WriteOperation) + Send;

    /// Forces pending writes to be physically recorded on disk.
    fn fsync(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOperation {
    pub op: WriteKind,
    pub offset: u64,
}

/// Operations that appear in the Append-Only File (AOF).
/// Client request is converted to WriteOperation and then it turns into WriteOp when it gets offset
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteKind {
    /// Set a `key` to `value`, optionally with an expiration epoch time.
    /// TODO: Add `expires_at`.
    Set {
        key: String,
        value: String,
    },

    SetWithExpiry {
        key: String,
        value: String,
        expires_at: u64,
    },
    /// Delete a key.
    Delete {
        key: String,
    },
}

impl WriteOperation {
    pub fn serialize(self) -> Bytes {
        QueryIO::Replicate { query: self.op, offset: self.offset }.serialize()
    }
}

impl WriteKind {
    /// Serialize this `WriteOperation` into bytes.
    pub fn to_array(self) -> QueryIO {
        match self {
            WriteKind::Set { key, value } => write_array!("SET", key, value),
            WriteKind::SetWithExpiry { key, value, expires_at } => {
                write_array!("SET", key, value, "px", expires_at.to_string())
            }
            WriteKind::Delete { key } => write_array!("DEL", key),
        }
    }

    /// Deserialize `WriteOperation`s from the given bytes.
    pub fn deserialize(mut bytes: BytesMut) -> Result<Vec<WriteOperation>> {
        let mut ops = Vec::new();

        while !bytes.is_empty() {
            let (query, consumed) = deserialize_query_io(bytes.clone())?;
            bytes = bytes.split_off(consumed);

            let (op, offset) = match query {
                QueryIO::Replicate { query, offset } => (query, offset),
                _ => return Err(anyhow::anyhow!("expected replicate")),
            };

            ops.push(WriteOperation { op: op, offset: offset });
        }

        Ok(ops)
    }

    pub fn from_client_req(req: &ClientRequest) -> Option<Self> {
        match req {
            ClientRequest::Set { key, value } => {
                Some(WriteKind::Set { key: key.clone(), value: value.clone() })
            }
            ClientRequest::SetWithExpiry { key, value, expiry } => {
                let expires_at =
                    expiry.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis()
                        as u64;

                Some(WriteKind::SetWithExpiry {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at,
                })
            }
            _ => None,
        }
    }
}

impl TryFrom<QueryIO> for WriteKind {
    type Error = anyhow::Error;

    fn try_from(query: QueryIO) -> Result<Self> {
        match query {
            QueryIO::Array(bulk_strings) => {
                if bulk_strings.len() < 3 {
                    return Err(anyhow::anyhow!("expected array"));
                }

                let (
                    QueryIO::BulkString(cmd_bytes),
                    QueryIO::BulkString(key_bytes),
                    QueryIO::BulkString(value_bytes),
                    px,
                    expiry,
                ) = (
                    bulk_strings.first().unwrap(),
                    bulk_strings.get(1).unwrap(),
                    bulk_strings.get(2).unwrap(),
                    bulk_strings.get(3),
                    bulk_strings.get(4),
                )
                else {
                    return Err(anyhow::anyhow!("expected bulk string"));
                };

                let cmd = std::str::from_utf8(&cmd_bytes)?;

                let op = match cmd {
                    "SET" if bulk_strings.len() == 3 => {
                        let key = String::from_utf8(key_bytes.to_vec())?;
                        let value = String::from_utf8(value_bytes.to_vec())?;
                        WriteKind::Set { key, value }
                    }
                    _ => todo!(),
                };

                Ok(op)
            }
            _ => Err(anyhow::anyhow!("expected array")),
        }
    }
}
