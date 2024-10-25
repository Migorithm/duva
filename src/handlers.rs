use std::sync::Arc;

use crate::{
    adapters::in_memory::InMemoryDb,
    config::Config,
    interface::{Database, TRead, TWriteBuf},
    protocol::{command::Args, value::Value, MessageParser},
};
use anyhow::Result;

pub(crate) struct Handler {
    pub(crate) conf: Arc<Config>,
}

impl Handler {
    pub async fn handle<T: TWriteBuf + TRead>(
        &mut self,
        resp_handler: &mut MessageParser<T>,
    ) -> Result<()> {
        let Some(v) = resp_handler.read_operation().await? else {
            return Err(anyhow::anyhow!("Connection closed"));
        };

        let (command, args) = Args::extract_command(v)?;

        let response = match command.as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first()?,
            "set" => Handler::handle_set(&args, InMemoryDb).await?,
            "get" => Handler::handle_get(&args, InMemoryDb).await?,
            // modify we have to add a new command
            "config" => self.handle_config(&args)?,
            c => panic!("Cannot handle command {}", c),
        };

        resp_handler.write_value(response).await?;
        Ok(())
    }

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

    // perhaps, set operation is needed
    pub fn handle_config(&mut self, args: &Args) -> Result<Value> {
        let sub_command = args.first()?;
        let args = &args.0[1..];

        let (Value::BulkString(command), [Value::BulkString(key), ..]) = (&sub_command, args)
        else {
            println!("subcommand {:?}", sub_command);
            println!("dddd {args:?}");
            return Err(anyhow::anyhow!("Invalid arguments"));
        };

        match (command.as_str(), key.as_str()) {
            ("get" | "GET", "dir") => Ok(Value::Array(vec![
                Value::BulkString("dir".to_string()),
                self.conf
                    .dir
                    .clone()
                    .map(|v| Value::BulkString(v))
                    .unwrap_or(Value::Null),
            ])),
            ("get" | "GET", "dbfilename") => Ok(Value::Array(vec![
                Value::BulkString("dbfilename".to_string()),
                self.conf
                    .db_filename
                    .clone()
                    .map(|v| Value::BulkString(v))
                    .unwrap_or(Value::Null),
            ])),
            _ => Err(anyhow::anyhow!("Invalid arguments")),
        }
    }
}
