use anyhow::Result;
use bytes::{Bytes, BytesMut};

use crate::write_array;

use super::query_io::{deserialize as deserialize_query_io, QueryIO};

/// Trait for an Append-Only File (AOF) abstraction.
pub trait TAof {
    /// Appends a single `WriteOperation` to the log.
    fn append(
        &mut self,
        op: &WriteOperation,
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

/// Operations that appear in the Append-Only File (AOF).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOperation {
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
    /// Serialize this `WriteOperation` into bytes.
    pub fn serialize(&self) -> Bytes {
        match &self {
            WriteOperation::Set { key, value } => {
                let bytes = write_array!("SET", key.clone(), value.clone()).serialize();

                bytes
            }

            // TODO deserialize this
            WriteOperation::SetWithExpiry { key, value, expires_at } => {
                let bytes =
                    write_array!("SET", key.clone(), value.clone(), "px", expires_at.to_string())
                        .serialize();
                bytes
            }
            WriteOperation::Delete { key } => todo!(),
        }
    }

    /// Deserialize `WriteOperation`s from the given bytes.
    pub fn deserialize(mut bytes: BytesMut) -> Result<Vec<WriteOperation>> {
        let mut ops = Vec::new();

        while !bytes.is_empty() {
            let (query, consumed) = deserialize_query_io(bytes.clone())?;
            bytes = bytes.split_off(consumed);

            let QueryIO::Array(bulk_strings) = query else {
                return Err(anyhow::anyhow!("expected array"));
            };

            let Some(QueryIO::BulkString(first_bytes)) = bulk_strings.first() else {
                return Err(anyhow::anyhow!("expected bulk string"));
            };

            let first_str = std::str::from_utf8(first_bytes)?;

            let op = match first_str {
                "SET" if bulk_strings.len() == 3 => {
                    let QueryIO::BulkString(key_bytes) = &bulk_strings[1] else {
                        return Err(anyhow::anyhow!("expected key"));
                    };
                    let QueryIO::BulkString(value_bytes) = &bulk_strings[2] else {
                        return Err(anyhow::anyhow!("expected value"));
                    };

                    let key = String::from_utf8(key_bytes.to_vec())?;
                    let value = String::from_utf8(value_bytes.to_vec())?;

                    WriteOperation::Set { key, value }
                }
                _ => todo!(),
            };

            ops.push(op);
        }

        Ok(ops)
    }
}

impl TryFrom<QueryIO> for WriteOperation {
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
                        WriteOperation::Set { key, value }
                    }
                    _ => todo!(),
                };

                Ok(op)
            }
            _ => Err(anyhow::anyhow!("expected array")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_operation_serialize_set() {
        // GIVEN a WriteOperation with a SET operation.
        let op = WriteOperation::Set { key: "foo".into(), value: "bar".into() };

        // WHEN serialized.
        let bytes = op.serialize();

        // THEN
        assert_eq!(bytes, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_write_operation_deserialize_set() {
        // GIVEN a serialized WriteOperation.
        let cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let bytes = BytesMut::from(cmd.as_bytes());

        // WHEN deserialized.
        let ops = WriteOperation::deserialize(bytes).unwrap();

        // THEN we get the expected WriteOperation.
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0], WriteOperation::Set { key: "foo".into(), value: "bar".into() });
    }
}
