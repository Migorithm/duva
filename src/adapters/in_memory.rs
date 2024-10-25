use crate::interface::Database;
use crate::protocol::value::Value;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

static DB: OnceLock<RwLock<HashMap<String, StoredValue>>> = OnceLock::new();

fn db() -> &'static RwLock<HashMap<String, StoredValue>> {
    DB.get_or_init(|| RwLock::new(HashMap::new()))
}

const NO_EXPIRY: Expiry = Expiry{expiry_seconds:0,expiry_nanos:0};// 20 bytes

#[derive(Debug,PartialEq)]
struct Expiry {
    expiry_seconds: u64,
    expiry_nanos: u32
}

struct StoredValue {
    value : String,
    expiry: Expiry
}

#[derive(Clone)]
pub struct InMemoryDb;

impl Database for InMemoryDb {
    async fn set(&self, key: String, value: String) {
        let mut guard = db().write().await;
        guard.insert(key, StoredValue{ value, expiry: Expiry { expiry_seconds:0, expiry_nanos:0} });
    }

    async fn set_with_expiration(&self, key: String, value: String, expiry: &Value) {
        // get expiry
        let Value::BulkString(expiry) = expiry else {
            //TODO implement the following
            unimplemented!();
        };
        let (secs, nanos) = calculate_expire_at(&expiry).unwrap();
        let mut guard = db().write().await;
        guard.insert(key, StoredValue { value, expiry:Expiry {expiry_seconds:secs, expiry_nanos:nanos} });
    }

    // Get a value
    async fn get(&self, key: &str) -> Option<String> {
        match db().read().await.get(key) {
            Some(value) => {
                if value.expiry == NO_EXPIRY {
                    return Some(value.value.to_string());
                }
                // get system time from expiry_in_sec and expiry_in_nanos
                if check_if_expired(&value.expiry) {
                    return None;
                }
                Some(value.value.to_string())
            }
            None => None,
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

use std::time::UNIX_EPOCH;

fn calculate_expire_at(expire_in: &str) -> Result<(u64, u32)> {
    let expire_in = expire_in.parse::<u64>().unwrap();
    let now = SystemTime::now();
    let expire_at = now + Duration::from_millis(expire_in);

    let duration = expire_at.duration_since(UNIX_EPOCH)?;
    Ok((duration.as_secs(), duration.subsec_nanos()))
}

fn check_if_expired(expiry: &Expiry) -> bool {
    let expiry_in_sec = expiry.expiry_seconds;
    let expiry_in_nanos = expiry.expiry_nanos;
    let expire_at = Duration::new(expiry_in_sec, expiry_in_nanos);

    // get current time
    let now = SystemTime::now();
    let now = now.duration_since(UNIX_EPOCH).unwrap();

    now > expire_at
}

#[test]
fn test_check_if_expired() {
    let expire_in = "100";
    let (secs, nanos) = calculate_expire_at(expire_in).unwrap();

    let expiry = Expiry { expiry_seconds: secs, expiry_nanos: nanos };
    assert_eq!(check_if_expired(&expiry), false);
}

#[test]
fn test_value_equality() {
    let expiry1 = Expiry { expiry_seconds: 0, expiry_nanos: 0 };
    let expiry2 = Expiry { expiry_seconds: 0, expiry_nanos: 0 };

    assert_eq!(expiry1,expiry2);
}