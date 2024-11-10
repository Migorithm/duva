pub mod command;

pub mod routers;
pub mod ttl_handlers;

use anyhow::Result;
use std::collections::HashMap;
use tokio::{sync::oneshot, time};
use ttl_handlers::set::TtlSetter;

use crate::make_smart_pointer;

use super::value::Value;

#[derive(Default)]
pub struct CacheDb(HashMap<String, String>);

impl CacheDb {
    pub async fn handle_set(
        &mut self,
        key: String,
        value: String,
        expiry: Option<u64>,
        ttl_sender: TtlSetter,
    ) -> Result<Value> {
        match expiry {
            Some(expiry) => {
                self.insert(key.clone(), value.clone());
                ttl_sender.set_ttl(key.clone(), expiry).await;
            }
            None => {
                self.insert(key.clone(), value.clone());
            }
        }
        Ok(Value::SimpleString("OK".to_string()))
    }

    pub fn handle_get(&self, key: String, sender: oneshot::Sender<Value>) {
        let _ = sender.send(self.get(&key).cloned().into());
    }

    fn handle_delete(&mut self, key: &str) {
        self.remove(key);
    }

    fn handle_keys(&self, pattern: Option<String>, sender: oneshot::Sender<Value>) {
        let ks = self
            .keys()
            .filter(|k| match &pattern {
                // TODO better way to to matching?
                Some(pattern) => k.contains(pattern),
                None => true,
            })
            .map(|k| Value::BulkString(k.clone()))
            .collect::<Vec<_>>();
        sender.send(Value::Array(ks)).unwrap();
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
