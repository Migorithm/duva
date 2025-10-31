pub mod snapshot_loader;
use crate::domains::{caches::cache_objects::CacheEntry, replications::ReplicationId};

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct Snapshot {
    pub(crate) header: String,
    pub(crate) metadata: Metadata,
    pub(crate) database: Vec<SubDatabase>,
    pub(crate) checksum: Vec<u8>,
}

impl Snapshot {
    pub fn new(
        header: String,
        metadata: Metadata,
        database: Vec<SubDatabase>,
        checksum: Vec<u8>,
    ) -> Self {
        Self { header, metadata, database, checksum }
    }
    pub fn key_values(self) -> Vec<CacheEntry> {
        self.database.into_iter().flat_map(|section| section.storage.into_iter()).collect()
    }

    pub(crate) fn extract_replication_info(&self) -> (ReplicationId, u64) {
        (self.metadata.repl_id.clone(), self.metadata.log_idx)
    }

    pub(crate) fn default_with_repl_id(repl_id: ReplicationId) -> Self {
        Self { metadata: Metadata { repl_id, log_idx: Default::default() }, ..Default::default() }
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct Metadata {
    pub(crate) repl_id: ReplicationId,
    pub(crate) log_idx: u64,
}

#[derive(Debug)]
pub struct SubDatabase {
    pub index: usize,
    pub storage: Vec<CacheEntry>,
}
