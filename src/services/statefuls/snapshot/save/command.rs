use crate::domains::storage::cache_objects::CacheEntry;

pub enum SaveCommand {
    LocalShardSize { table_size: usize, expiry_size: usize },
    SaveChunk(Vec<CacheEntry>),
    StopSentinel,
}
