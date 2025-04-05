use crate::{
    domains::query_parsers::{QueryIO, deserialize},
    write_array,
};
use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct WriteOperation {
    pub(crate) request: WriteRequest,
    pub(crate) log_index: u64,
    pub(crate) term: u64,
}

/// Operations that appear in the Append-Only File (WAL).
/// Client request is converted to WriteOperation and then it turns into WriteOp when it gets offset
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum WriteRequest {
    Set { key: String, value: String },
    SetWithExpiry { key: String, value: String, expires_at: u64 },
    Delete { keys: Vec<String> },
}

impl WriteOperation {
    pub fn serialize(self) -> Bytes {
        QueryIO::WriteOperation(self).serialize()
    }
}

impl WriteRequest {
    pub(crate) fn key(&self) -> Vec<String> {
        match self {
            WriteRequest::Set { key, .. } => vec![key.clone()],
            WriteRequest::SetWithExpiry { key, .. } => vec![key.clone()],
            WriteRequest::Delete { keys: key } => key.clone(),
        }
    }
    pub fn to_array(self) -> QueryIO {
        match self {
            WriteRequest::Set { key, value } => write_array!("SET", key, value),
            WriteRequest::SetWithExpiry { key, value, expires_at } => {
                write_array!("SET", key, value, "px", expires_at.to_string())
            },
            WriteRequest::Delete { keys: key } => {
                let mut args = vec!["DEL".to_string()];
                args.extend(key);
                QueryIO::Array(args.into_iter().map(QueryIO::BulkString).collect())
            },
        }
    }

    /// Deserialize `WriteOperation`s from the given bytes.
    pub fn deserialize(mut bytes: BytesMut) -> anyhow::Result<Vec<WriteOperation>> {
        let mut ops: Vec<WriteOperation> = Vec::new();

        while !bytes.is_empty() {
            let (query, consumed) = deserialize(bytes.clone())?;
            bytes = bytes.split_off(consumed);

            let QueryIO::WriteOperation(write_operation) = query else {
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

                Ok(WriteRequest::Set { key, value })
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

                Ok(WriteRequest::SetWithExpiry { key, value, expires_at: expiry.parse()? })
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
