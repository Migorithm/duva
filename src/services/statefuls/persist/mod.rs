pub mod actor;
pub mod encoding_command;
pub mod endec;
use super::cache::CacheEntry;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DumpFile {
    pub(crate) header: String,
    pub(crate) metadata: DumpMetadata,
    pub(crate) database: Vec<DatabaseSection>,
    pub(crate) checksum: Vec<u8>,
}

impl DumpFile {
    pub fn new(
        header: String,
        metadata: DumpMetadata,
        database: Vec<DatabaseSection>,
        checksum: Vec<u8>,
    ) -> Self {
        Self { header, metadata, database, checksum }
    }
    pub fn key_values(self) -> Vec<CacheEntry> {
        self.database.into_iter().flat_map(|section| section.storage.into_iter()).collect()
    }

    pub fn extract_replication_info(&self) -> Option<(String, u64)> {
        if let Some(repl_id) = self.metadata.repl_id.clone() {
            if let Some(offset) = self.metadata.repl_offset {
                let offset: u64 = offset;

                println!("[INFO] Replication info set from dump file");
                return Some((repl_id.to_string(), offset));
            }
        }
        None
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct DumpMetadata {
    pub(crate) repl_id: Option<String>,
    pub(crate) repl_offset: Option<u64>,
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
