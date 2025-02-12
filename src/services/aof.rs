use super::client::request::ClientRequest;
use super::query_io::deserialize as deserialize_query_io;
use super::query_io::QueryIO;
use crate::write_array;
use anyhow::Result;
use bytes::{Bytes, BytesMut};

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
    pub op: WriteRequest,
    pub offset: u64,
}

/// Operations that appear in the Append-Only File (AOF).
/// Client request is converted to WriteOperation and then it turns into WriteOp when it gets offset
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRequest {
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
        QueryIO::ReplicateLog(self).serialize()
    }
}

impl WriteRequest {
    pub fn to_array(self) -> QueryIO {
        match self {
            WriteRequest::Set { key, value } => write_array!("SET", key, value),
            WriteRequest::SetWithExpiry { key, value, expires_at } => {
                write_array!("SET", key, value, "px", expires_at.to_string())
            }
            WriteRequest::Delete { key } => write_array!("DEL", key),
        }
    }

    /// Deserialize `WriteOperation`s from the given bytes.
    pub fn deserialize(mut bytes: BytesMut) -> Result<Vec<WriteOperation>> {
        let mut ops = Vec::new();

        while !bytes.is_empty() {
            let (query, consumed) = deserialize_query_io(bytes.clone())?;
            bytes = bytes.split_off(consumed);

            let QueryIO::ReplicateLog(write_operation) = query else {
                return Err(anyhow::anyhow!("expected replicate"));
            };
            ops.push(write_operation);
        }

        Ok(ops)
    }

    pub fn from_client_req(req: &ClientRequest) -> Option<Self> {
        match req {
            ClientRequest::Set { key, value } => {
                Some(WriteRequest::Set { key: key.clone(), value: value.clone() })
            }
            ClientRequest::SetWithExpiry { key, value, expiry } => {
                let expires_at =
                    expiry.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis()
                        as u64;

                Some(WriteRequest::SetWithExpiry {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at,
                })
            }
            _ => None,
        }
    }

    pub fn new(cmd: String, args: std::vec::IntoIter<QueryIO>) -> Result<Self> {
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
            }

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
            }

            _ => Err(anyhow::anyhow!("expected 2 or 4 arguments")),
        }
    }
}

impl From<WriteOperation> for Bytes {
    fn from(op: WriteOperation) -> Self {
        op.serialize()
    }
}
