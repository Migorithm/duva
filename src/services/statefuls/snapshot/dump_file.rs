use crate::services::statefuls::cache::CacheEntry;

#[derive(Debug)]
pub struct DumpFile {
    pub(crate) header: String,
    pub(crate) metadata: DecodedMetadata,
    pub(crate) database: Vec<DecodedDatabase>,
    pub(crate) checksum: Vec<u8>,
}

impl DumpFile {
    pub fn new(
        header: String,
        metadata: DecodedMetadata,
        database: Vec<DecodedDatabase>,
        checksum: Vec<u8>,
    ) -> Self {
        Self { header, metadata, database, checksum }
    }
    pub fn key_values(self) -> Vec<CacheEntry> {
        self.database.into_iter().flat_map(|section| section.storage.into_iter()).collect()
    }

    pub fn extract_replication_info(&self) -> Option<(String, u64)> {
        match (self.metadata.repl_id.as_ref(), self.metadata.repl_offset) {
            (Some(repl_id), Some(offset)) => Some((repl_id.clone(), offset)),
            _ => None,
        }
    }
}

// TODO make it non-nullable?
#[derive(Debug, Default, PartialEq)]
pub struct DecodedMetadata {
    pub(crate) repl_id: Option<String>,
    pub(crate) repl_offset: Option<u64>,
}

#[derive(Debug)]
pub struct DecodedDatabase {
    pub index: usize,
    pub storage: Vec<CacheEntry>,
}
