use crate::domains::{
    QueryIO, caches::cache_objects::CacheEntry, cluster_actors::SessionRequest, deserialize,
};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct WriteOperation {
    pub(crate) request: WriteRequest,
    pub(crate) log_index: u64,
    pub(crate) term: u64,
    pub(crate) session_req: Option<SessionRequest>,
}

/// Operations that appear in the Append-Only File (WAL).
/// Client request is converted to WriteOperation and then it turns into WriteOp when it gets offset
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum WriteRequest {
    Set { key: String, value: String, expires_at: Option<u64> },
    MSet { entries: Vec<CacheEntry> },
    Delete { keys: Vec<String> },
    Append { key: String, value: String },
    Decr { key: String, delta: i64 },
    Incr { key: String, delta: i64 },
    LPush { key: String, value: Vec<String> },
    LPop { key: String, count: usize },
    RPush { key: String, value: Vec<String> },
    LTrim { key: String, start: isize, end: isize },
}

impl WriteOperation {
    pub(crate) fn serialize(self) -> Bytes {
        QueryIO::WriteOperation(self).serialize()
    }
}

impl WriteRequest {
    /// Deserialize `WriteOperation`s from the given bytes.
    pub(crate) fn deserialize(bytes: impl Into<Bytes>) -> anyhow::Result<Vec<WriteOperation>> {
        let mut ops: Vec<WriteOperation> = Vec::new();
        let mut bytes = bytes.into();

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

    /// Returns all keys involved in the operation.
    pub(crate) fn all_keys(&self) -> Vec<&str> {
        match self {
            | WriteRequest::Set { key, .. } => vec![key],
            | WriteRequest::Append { key, .. } => vec![key],
            | WriteRequest::Incr { key, .. } => vec![key],
            | WriteRequest::Decr { key, .. } => vec![key],
            | WriteRequest::Delete { keys, .. } => keys.iter().map(|k| k.as_str()).collect(),
            | WriteRequest::MSet { entries } => entries.iter().map(|e| e.key()).collect(),
            | WriteRequest::LPush { key, .. } => vec![key],
            | WriteRequest::LPop { key, .. } => vec![key],
            | WriteRequest::RPush { key, .. } => vec![key],
            | WriteRequest::LTrim { key, .. } => vec![key],
        }
    }
}
