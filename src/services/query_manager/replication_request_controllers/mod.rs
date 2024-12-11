use arguments::ReplicationRequestArguments;
use replication_request::ReplicationRequest;

use crate::services::config::{config_manager::ConfigManager, ConfigCommand};

use super::query_io::QueryIO;

pub mod arguments;
pub mod replication_request;

pub struct ReplicationRequestController {
    config_manager: ConfigManager,
}

impl ReplicationRequestController {
    pub fn new(config_manager: ConfigManager) -> Self {
        Self { config_manager }
    }
    pub async fn handle(
        &self,
        request: ReplicationRequest,
        args: ReplicationRequestArguments,
    ) -> anyhow::Result<QueryIO> {
        // handle replication request
        let response = match request {
            ReplicationRequest::Ping => {
                // ! HACK to test out if ping was given from replica
                self.config_manager
                    .route_command(ConfigCommand::ReplicaPing)
                    .await?;
                QueryIO::SimpleString("PONG".to_string())
            }
            ReplicationRequest::ReplConf => {
                // args will be:
                // "listening-port" "port" OR
                // "capa" "eof" "capa" "psync2"
                QueryIO::BulkString("OK".to_string())
            }

            ReplicationRequest::Psync => QueryIO::BulkString(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
            ),
        };
        Ok(response)
    }
}
