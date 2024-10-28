use std::sync::Arc;

use crate::config::Config;

use super::query_manager::{query::Args, value::Value};
use anyhow::Result;
pub(crate) struct ConfigHandler {
    pub(crate) conf: Arc<Config>,
}

impl ConfigHandler {
    pub fn new(conf: Arc<Config>) -> Self {
        ConfigHandler { conf }
    }

    // perhaps, set operation is needed
    pub fn handle_config(&mut self, args: &Args) -> Result<Value> {
        let sub_command = args.first()?;
        let args = &args.0[1..];

        let (Value::BulkString(command), [Value::BulkString(key), ..]) = (&sub_command, args)
        else {
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
