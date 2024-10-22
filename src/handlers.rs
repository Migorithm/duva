use crate::{
    interface::Database,
    protocol::{command::Args, value::Value},
};
use anyhow::Result;

pub struct Handler;

impl Handler {
    pub async fn handle_set(args: &Args, db: impl Database) -> Result<Value> {
        let key = args.first();
        let value = args.0.get(1);

        match (key?, value) {
            (Value::BulkString(key), Some(Value::BulkString(value))) => {
                db.set(key.clone(), value.clone()).await;
            }
            _ => return Err(anyhow::anyhow!("Invalid arguments")),
        }

        Ok(Value::SimpleString("OK".to_string()))
    }

    pub async fn handle_get(args: &Args, db: impl Database) -> Result<Value> {
        let Value::BulkString(key) = args.first()? else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        let value = db.get(&key).await;

        Ok(Value::BulkString(value.unwrap_or_default()))
    }
}
