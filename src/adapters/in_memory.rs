use std::collections::HashMap;

use std::sync::OnceLock;

use tokio::sync::RwLock;

use crate::interface::Database;

static DB: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

fn db() -> &'static RwLock<HashMap<String, String>> {
    DB.get_or_init(|| RwLock::new(HashMap::new()))
}

pub struct InMemoryDb;

impl Database for InMemoryDb {
    async fn set(&self, key: String, value: String) {
        // Perform the write operation
        let mut guard = db().write().await;
        guard.insert(key, value);
    }

    // Get a value
    async fn get(&self, key: &str) -> Option<String> {
        db().read().await.get(key).cloned()
    }
}

impl InMemoryDb {
    // Delete a value with atomic write access
    pub async fn delete(&self, key: &str) -> bool {
        let mut guard = db().write().await;
        guard.remove(key).is_some()
    }

    // Get all keys (for debugging/monitoring)
    pub async fn get_keys(&self) -> Vec<String> {
        db().read().await.keys().cloned().collect()
    }
}
