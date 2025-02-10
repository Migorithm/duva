use crate::services::statefuls::cache::CacheEntry;

pub enum EncodingCommand {
    LocalShardSize { table_size: usize, expiry_size: usize },
    SaveChunk(Vec<CacheEntry>),
    StopSentinel,
}
