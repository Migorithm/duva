pub mod endec;
pub mod save_actor;

use std::collections::HashMap;

use super::cache::CacheEntry;

#[derive(Debug)]
pub struct RdbFile {
    pub(crate) header: String,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) database: Vec<DatabaseSection>,
    pub(crate) checksum: Vec<u8>,
}

impl RdbFile {
    pub fn new(
        header: String,
        metadata: HashMap<String, String>,
        database: Vec<DatabaseSection>,
        checksum: Vec<u8>,
    ) -> Self {
        Self {
            header,
            metadata,
            database,
            checksum,
        }
    }
    pub fn key_values(self) -> Vec<CacheEntry> {
        self.database
            .into_iter()
            .flat_map(|section| section.storage.into_iter())
            .collect()
    }
}

#[derive(Debug)]
pub struct DatabaseSection {
    pub index: usize,
    pub storage: Vec<CacheEntry>,
}
