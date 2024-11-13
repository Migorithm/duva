use std::collections::HashMap;

use crate::services::statefuls::routers::cache_actor::CacheDb;

pub mod size_encoding;
mod key_value_storage_extractor;
mod database_extractor;

pub struct RdbFile {
    header: String,
    metadata: HashMap<String,String>,
    database: Vec<CacheDb>,
}
