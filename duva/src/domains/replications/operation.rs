use crate::domains::{
    QueryIO, caches::cache_objects::CacheEntry, cluster_actors::SessionRequest, deserialize,
};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct WriteOperation {
    pub(crate) entry: LogEntry,
    pub(crate) log_index: u64,
    pub(crate) term: u64,
    pub(crate) session_req: Option<SessionRequest>,
}

/// Operations that appear in the Append-Only File (WAL).
/// Client request is converted to WriteOperation and then it turns into WriteOp when it gets offset
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum LogEntry {
    Set { entry: CacheEntry },
    MSet { entries: Vec<CacheEntry> },
    Delete { keys: Vec<String> },
    Append { key: String, value: String },
    DecrBy { key: String, delta: i64 },
    IncrBy { key: String, delta: i64 },
    LPush { key: String, value: Vec<String> },
    LPushX { key: String, value: Vec<String> },
    LPop { key: String, count: usize },
    RPush { key: String, value: Vec<String> },
    RPop { key: String, count: usize },
    RPushX { key: String, value: Vec<String> },
    LTrim { key: String, start: isize, end: isize },
    LSet { key: String, index: isize, value: String },
    NoOp,
}

impl LogEntry {
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
        use LogEntry::*;

        match self {
            Set { entry } => vec![entry.key()],
            Append { key, .. }
            | IncrBy { key, .. }
            | DecrBy { key, .. }
            | LPush { key, .. }
            | LPop { key, .. }
            | RPush { key, .. }
            | LTrim { key, .. }
            | LPushX { key, .. }
            | LSet { key, .. }
            | RPop { key, .. }
            | RPushX { key, .. } => vec![key],
            Delete { keys, .. } => keys.iter().map(|k| k.as_str()).collect(),
            MSet { entries } => entries.iter().map(|e| e.key()).collect(),
            NoOp => vec![],
        }
    }
}
