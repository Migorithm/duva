pub mod actor;
pub mod endec;
pub mod encoding_command;
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
