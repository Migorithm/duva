use std::collections::VecDeque;

use crate::{domains::storage::cache_objects::CacheEntry, services::error::IoError};
use tokio::io::AsyncWriteExt;

pub struct SaveActor {
    pub(crate) target: SaveTarget,
    pub(crate) meta: SaveMeta,
}

pub enum SaveTarget {
    File(tokio::fs::File),
    InMemory(Vec<u8>),
}

impl SaveTarget {
    pub async fn write(&mut self, buf: &[u8]) -> Result<(), IoError> {
        match self {
            SaveTarget::File(f) => f.write_all(buf).await.map_err(|e| e.kind().into()),
            SaveTarget::InMemory(v) => {
                v.extend_from_slice(buf);
                Ok(())
            }
        }
    }
}

pub struct SaveMeta {
    pub(crate) num_of_saved_table_size_actor: usize,
    pub(crate) total_key_value_table_size: usize,
    pub(crate) total_expires_table_size: usize,
    pub(crate) chunk_queue: VecDeque<Vec<CacheEntry>>,
    pub(crate) num_of_cache_actors: usize,
    pub(crate) repl_id: String,
    pub(crate) offset: String,
}

impl SaveMeta {
    pub fn new(num_of_cache_actors: usize, repl_id: String, offset: String) -> Self {
        Self {
            num_of_saved_table_size_actor: num_of_cache_actors,
            total_key_value_table_size: 0,
            total_expires_table_size: 0,
            chunk_queue: VecDeque::new(),
            num_of_cache_actors,
            repl_id,
            offset,
        }
    }
}
