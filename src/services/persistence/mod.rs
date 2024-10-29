pub mod command;

pub mod router;
pub mod ttl_handlers;

use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::oneshot;
use ttl_handlers::set::TtlSetter;

use crate::{make_smart_pointer, services::value::Values};

use super::value::Value;

#[derive(Default)]
struct CacheDb(HashMap<String, String>);

impl CacheDb {
    pub async fn handle_set(&mut self, args: &Values, ttl_sender: TtlSetter) -> Result<Value> {
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

    pub fn handle_get(&self, args: &Values, sender: oneshot::Sender<Value>) {
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

make_smart_pointer!(CacheDb, HashMap<String, String>);
