use crate::services::{statefuls::persist::DatabaseSection, CacheEntry};

#[derive(Default)]
pub struct DatabaseSectionBuilder {
    pub(super) index: usize,
    pub(super) storage: Vec<CacheEntry>,
    pub(super) key_value_table_size: usize,
    pub(super) expires_table_size: usize,
}
impl DatabaseSectionBuilder {
    pub fn build(self) -> DatabaseSection {
        DatabaseSection {
            index: self.index,
            storage: self.storage,
        }
    }
}
