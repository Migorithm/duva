use crate::domains::{caches::cache_objects::CacheEntry, peers::identifier::PeerIdentifier};

#[derive(Debug)]
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

    pub fn extract_replication_info(&self) -> (PeerIdentifier, u64) {
        (self.metadata.repl_id.clone().into(), self.metadata.repl_offset)
    }
}

#[derive(Debug, PartialEq, Default)]
pub struct Metadata {
    pub(crate) repl_id: PeerIdentifier,
    pub(crate) repl_offset: u64,
}

#[derive(Debug)]
pub struct SubDatabase {
    pub index: usize,
    pub storage: Vec<CacheEntry>,
}
