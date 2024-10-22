use std::collections::HashMap;

use std::sync::LazyLock;

use tokio::sync::RwLock;

use crate::interface::Database;

static DB: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub struct InMemoryDb;

impl Database for InMemoryDb {
    async fn set(&self, key: String, value: String) {
        // Perform the write operation
        let mut guard = DB.write().await;
        guard.insert(key, value);
    }

    // Get a value
    async fn get(&self, key: &str) -> Option<String> {
        DB.read().await.get(key).cloned()
    }
}

impl InMemoryDb {
    // Delete a value with atomic write access
    pub async fn delete(&self, key: &str) -> bool {
        let mut guard = DB.write().await;
        guard.remove(key).is_some()
    }

    // Get all keys (for debugging/monitoring)
    pub async fn get_keys(&self) -> Vec<String> {
        DB.read().await.keys().cloned().collect()
    }
}
