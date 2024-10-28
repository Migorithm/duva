use crate::services::interface::Database;
use crate::services::query_manager::value::Value;

use std::collections::HashMap;
use std::sync::OnceLock;
use tokio::sync::RwLock;

static DB: OnceLock<RwLock<HashMap<String, StoredValue>>> = OnceLock::new();

fn db() -> &'static RwLock<HashMap<String, StoredValue>> {
    DB.get_or_init(|| RwLock::new(HashMap::new()))
}

struct StoredValue {
    value: String,
}

#[derive(Clone)]
pub struct InMemoryDb;

impl Database for InMemoryDb {
    async fn set(&self, key: String, value: String) {
        let mut guard = db().write().await;
        guard.insert(key, StoredValue { value });
    }

    async fn set_with_expiration(&self, key: String, value: String, expiry: &Value) {
        // get expiry
        let Value::BulkString(expiry) = expiry else {
            //TODO implement the following
            unimplemented!();
        };

        let mut guard = db().write().await;
        guard.insert(key, StoredValue { value });
    }

    // Get a value
    async fn get(&self, key: &str) -> Option<String> {
        let guard = db().read().await;
        Some(guard.get(key)?.value.clone())
    }

    async fn delete(&self, key: &str) {
        let mut guard = db().write().await;
        guard.remove(key);
    }
}

impl InMemoryDb {
    // Get all keys (for debugging/monitoring)
    pub async fn get_keys(&self) -> Vec<String> {
        db().read().await.keys().cloned().collect()
    }
}
