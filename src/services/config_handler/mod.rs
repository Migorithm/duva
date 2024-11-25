pub mod command;
use crate::config::Config;
use std::sync::Arc;

use anyhow::Result;
use command::{ConfigCommand, ConfigResource};

use super::query_io::QueryIO;

#[derive(Clone)]
pub(crate) struct ConfigHandler {
    pub(crate) conf: Arc<Config>,
}

impl ConfigHandler {
    pub fn new(conf: Arc<Config>) -> Self {
        ConfigHandler { conf }
    }

    // perhaps, set operation is needed
    pub fn handle_config(&mut self, cmd: ConfigCommand) -> Result<QueryIO> {
        match cmd {
            ConfigCommand::Get(ConfigResource::Dir) => Ok(QueryIO::Array(vec![
                QueryIO::BulkString("dir".to_string()),
                self.get_dir(),
            ])),
            ConfigCommand::Get(ConfigResource::DbFileName) => Ok(QueryIO::Array(vec![
                QueryIO::BulkString("dbfilename".to_string()),
                self.get_db_filename(),
            ])),
        }
    }

    fn get_dir(&self) -> QueryIO {
        self.conf
            .dir
            .clone()
            .map(|v| QueryIO::BulkString(v))
            .unwrap_or(QueryIO::Null)
    }

    fn get_db_filename(&self) -> QueryIO {
        self.conf
            .db_filename
            .clone()
            .map(|v| QueryIO::BulkString(v))
            .unwrap_or(QueryIO::Null)
    }
}
