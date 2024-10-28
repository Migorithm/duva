use super::{
    interface::Database,
    query_manager::{
        command::Args,
        value::{TtlCommand, Value},
    },
};
use anyhow::Result;
use tokio::sync::mpsc::Sender;

pub(crate) struct PersistenceHandler<DB: Database> {
    pub(crate) db: DB,
    pub(crate) ttl_sender: Sender<TtlCommand>,
}

impl<DB: Database> PersistenceHandler<DB> {
    pub fn new(db: DB, ttl_sender: Sender<TtlCommand>) -> Self {
        PersistenceHandler { db, ttl_sender }
    }

    pub async fn handle_set(&self, args: &Args) -> Result<Value> {
        let (key, value, expiry) = args.take_set_args()?;

        match (key, value, expiry) {
            (Value::BulkString(key), Value::BulkString(value), Some(expiry)) => {
                self.db.set(key.clone(), value.clone()).await;

                self.ttl_sender
                    .send(TtlCommand::Expiry {
                        expiry: expiry.extract_expiry()?,
                        key: key.clone(),
                    })
                    .await?;
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
