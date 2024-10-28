use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc::Receiver, oneshot::Sender};

use super::query_manager::value::Value;

pub enum PersistEnum {
    Set(String, String),
    Get(String, Sender<Value>),
    StopSentinel,
}

pub async fn persist_actor(mut recv: Receiver<PersistEnum>) -> Result<()> {
    // inner state
    let mut db = HashMap::<String, String>::new();

    while let Some(command) = recv.recv().await {
        match command {
            PersistEnum::StopSentinel => break,
            PersistEnum::Set(k, v) => {
                db.insert(k, v);
            }
            PersistEnum::Get(key, sender) => {
                match sender.send(db.get(key.as_str()).cloned().into()) {
                    Ok(_) => {}
                    Err(err) => println!("Error: {:?}", err),
                }
            }
        }
    }
    Ok(())
}

impl From<Option<String>> for Value {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(v) => Value::BulkString(v),
            None => Value::Null,
        }
    }
}
