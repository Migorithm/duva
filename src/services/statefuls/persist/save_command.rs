use crate::services::statefuls::cache::CacheEntry;

pub enum SaveCommand {
    LocalShardSize {
        table_size: usize,
        expiry_size: usize,
    },
    SaveChunk(Vec<CacheEntry>),
    StopSentinel,
}