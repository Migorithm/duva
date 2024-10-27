use crate::services::interface::Database;
use crate::services::parser::value::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

static DB: OnceLock<RwLock<HashMap<String, StoredValue>>> = OnceLock::new();

fn db() -> &'static RwLock<HashMap<String, StoredValue>> {
    DB.get_or_init(|| RwLock::new(HashMap::new()))
}

struct StoredValue {
    value: String,
    expiry: Option<SystemTime>,
}

#[derive(Clone)]
pub struct InMemoryDb;

impl Database for InMemoryDb {
    async fn set(&self, key: String, value: String) {
        let mut guard = db().write().await;
        guard.insert(
            key,
            StoredValue {
                value,
                expiry: None,
            },
        );
    }

    async fn set_with_expiration(&self, key: String, value: String, expiry: &Value) {
        // get expiry
        let Value::BulkString(expiry) = expiry else {
            //TODO implement the following
            unimplemented!();
        };

        let mut guard = db().write().await;
        guard.insert(
            key,
            StoredValue {
                value,
                expiry: Some(
                    SystemTime::now() + Duration::from_millis(expiry.parse::<u64>().unwrap()),
                ),
            },
        );
    }

    // Get a value
    async fn get(&self, key: &str) -> Option<String> {
        let guard = db().read().await;
        let value = guard.get(key)?;

        match value.expiry {
            Some(expire_at) if check_if_expired(&expire_at) => None,
            _ => Some(value.value.to_string()),
        }
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

fn check_if_expired(expire_at: &SystemTime) -> bool {
    SystemTime::now() > *expire_at
}
