use arguments::ReplicationRequestArguments;
use replication_request::ReplicationRequest;

use crate::services::config::config_manager::ConfigManager;

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
        Ok(QueryIO::Null)
    }
}
