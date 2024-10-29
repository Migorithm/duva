pub mod command;

pub mod router;
pub mod ttl_handlers;

use super::query_manager::{query::Args, value::Value};
use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::oneshot;
use ttl_handlers::set::TtlSetter;

#[derive(Default)]
struct CacheDb(HashMap<String, String>);

impl CacheDb {
    pub async fn handle_set(&mut self, args: &Args, ttl_sender: TtlSetter) -> Result<Value> {
        let (key, value, expiry) = args.take_set_args()?;

        match (key, value, expiry) {
            (Value::BulkString(key), Value::BulkString(value), Some(expiry)) => {
                self.insert(key.clone(), value.clone());
                // TODO set ttl
                ttl_sender
                    .set_ttl(key.clone(), expiry.extract_expiry()?)
                    .await;
            }
            (Value::BulkString(key), Value::BulkString(value), None) => {
                self.insert(key.clone(), value.clone());
            }
            _ => return Err(anyhow::anyhow!("Invalid arguments")),
        }
        Ok(Value::SimpleString("OK".to_string()))
    }

    pub fn handle_get(&self, args: &Args, sender: oneshot::Sender<Value>) {
        let Ok(Value::BulkString(key)) = args.first() else {
            let _ = sender.send(Value::Err("NotFound".to_string()));
            return;
        };
        let _ = sender.send(self.get(&key).cloned().into());
    }

    fn handle_delete(&mut self, key: &str) {
        self.remove(key);
    }
}

impl From<Option<String>> for Value {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(v) => Value::BulkString(v),
            None => Value::Null,
        }
    }
}

impl std::ops::Deref for CacheDb {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for CacheDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
