pub mod command;
use crate::config::Config;
use std::sync::Arc;

use anyhow::Result;
use command::{ConfigCommand, ConfigResource};

use super::value::Value;

#[derive(Clone)]
pub(crate) struct ConfigHandler {
    pub(crate) conf: Arc<Config>,
}

impl ConfigHandler {
    pub fn new(conf: Arc<Config>) -> Self {
        ConfigHandler { conf }
    }

    // perhaps, set operation is needed
    pub fn handle_config(&mut self, cmd: ConfigCommand) -> Result<Value> {
        match cmd {
            ConfigCommand::Get(ConfigResource::Dir) => Ok(Value::Array(vec![
                Value::BulkString("dir".to_string()),
                self.get_dir(),
            ])),
            ConfigCommand::Get(ConfigResource::DbFileName) => Ok(Value::Array(vec![
                Value::BulkString("dbfilename".to_string()),
                self.get_db_filename(),
            ])),
        }
    }

    fn get_dir(&self) -> Value {
        self.conf
            .dir
            .clone()
            .map(|v| Value::BulkString(v))
            .unwrap_or(Value::Null)
    }

    fn get_db_filename(&self) -> Value {
        self.conf
            .db_filename
            .clone()
            .map(|v| Value::BulkString(v))
            .unwrap_or(Value::Null)
    }
}
