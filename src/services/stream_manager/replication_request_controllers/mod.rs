use arguments::PeerRequestArguments;
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
        _args: PeerRequestArguments,
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
        };
        Ok(response)
    }
}
