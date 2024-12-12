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
        request: &ReplicationRequest,
        args: ReplicationRequestArguments,
        replica_id: String,
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
                if args.first() == Some(&QueryIO::BulkString("listening-port".to_string())) {
                    let port = args.take_replica_port()?;
                    self.config_manager
                        .route_command(ConfigCommand::ReplicaConf(replica_id.to_string(), "listening-port".to_string(), port))
                        .await?;
                }
                QueryIO::BulkString("OK".to_string())
            }

            ReplicationRequest::Psync => QueryIO::BulkString(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
            ),
        };
        Ok(response)
    }

    pub async fn get_replica_port(&self, replica_id: String) -> anyhow::Result<String> {
        let single_replica_info = self.config_manager.single_replica_info(replica_id).await?;
        single_replica_info.get("listening-port").cloned().ok_or(anyhow::anyhow!("No port found"))
    }
}
