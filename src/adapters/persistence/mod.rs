use std::collections::HashMap;

use crate::services::statefuls::CacheDb;

pub mod size_encoding;

pub struct RdbFile {
    header: String,
    metadata: HashMap<String, String>,
    // ! Number of actors?
    database: Vec<CacheDb>,
}
