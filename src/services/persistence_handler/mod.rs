use super::{
    interface::Database,
    parser::{command::Args, value::Value},
};
use anyhow::Result;

pub(crate) struct PersistenceHandler<DB: Database> {
    pub(crate) db: DB,
}

impl<DB: Database> PersistenceHandler<DB> {
    pub fn new(db: DB) -> Self {
        PersistenceHandler { db }
    }

    pub async fn handle_set(&self, args: &Args) -> Result<Value> {
        let (key, value, expiry) = args.take_set_args()?;

        match (key, value, expiry) {
            (Value::BulkString(key), Value::BulkString(value), Some(expiry)) => {
                self.db
                    .set_with_expiration(key.clone(), value.clone(), expiry)
                    .await;
            }
            (Value::BulkString(key), Value::BulkString(value), None) => {
                self.db.set(key.clone(), value.clone()).await;
            }

            _ => return Err(anyhow::anyhow!("Invalid arguments")),
        }

        Ok(Value::SimpleString("OK".to_string()))
    }

    pub async fn handle_get(&self, args: &Args) -> Result<Value> {
        let Value::BulkString(key) = args.first()? else {
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        match self.db.get(&key).await {
            Some(v) => Ok(Value::BulkString(v)),
            None => Ok(Value::Null),
        }
    }
}
