pub mod actor;
pub mod encoding_command;
pub mod endec;
use super::cache::CacheEntry;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DumpFile {
    pub(crate) header: String,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) database: Vec<DatabaseSection>,
    pub(crate) checksum: Vec<u8>,
}

impl DumpFile {
    pub fn new(
        header: String,
        metadata: HashMap<String, String>,
        database: Vec<DatabaseSection>,
        checksum: Vec<u8>,
    ) -> Self {
        Self { header, metadata, database, checksum }
    }
    pub fn key_values(self) -> Vec<CacheEntry> {
        self.database.into_iter().flat_map(|section| section.storage.into_iter()).collect()
    }

    pub fn extract_replication_info(&self) -> Option<(String, u64)> {
        if let Some(repl_id) = self.metadata.get("repl-id") {
            if let Some(offset) = self.metadata.get("repl-offset") {
                let offset: u64 = offset.parse().unwrap_or(0);

                println!("[INFO] Replication info set from dump file");
                return Some((repl_id.to_string(), offset));
            }
        }
        None
    }
}

pub struct DumpMetadata {
    pub(crate) repl_id: String,
    pub(crate) repl_offset: u64,
}

#[derive(Debug)]
pub struct DatabaseSection {
    pub index: usize,
    pub storage: Vec<CacheEntry>,
}

#[derive(Default)]
pub(crate) struct DatabaseSectionBuilder {
    pub(crate) index: usize,
    pub(crate) storage: Vec<CacheEntry>,
    pub(crate) key_value_table_size: usize,
    pub(crate) expires_table_size: usize,
}
impl DatabaseSectionBuilder {
    pub fn build(self) -> DatabaseSection {
        DatabaseSection { index: self.index, storage: self.storage }
    }
}
