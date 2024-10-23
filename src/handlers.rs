use crate::{
    interface::Database,
    protocol::{command::Args, value::Value},
};
use anyhow::Result;

pub struct Handler;

impl Handler {
    pub async fn handle_set(args: &Args, db: impl Database) -> Result<Value> {
        let (key, value, expiry) = args.take_set_args()?;

        match (key, value, expiry) {
            (Value::BulkString(key), Value::BulkString(value), Some(expiry)) => {
                db.set_with_expiration(key.clone(), value.clone(), expiry)
                    .await;
            }
            (Value::BulkString(key), Value::BulkString(value), None) => {
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

        match db.get(&key).await {
            Some(v) => Ok(Value::BulkString(v)),
            None => Ok(Value::Null),
        }
    }
}
