use arguments::PeerRequestArguments;
use replication_request::ReplicationRequest;

use crate::services::config::{config_manager::ConfigManager, ConfigCommand};

use super::query_io::QueryIO;

pub mod arguments;
pub mod replication_request;

pub struct ReplicationRequestController {
    pub(crate) config_manager: ConfigManager,
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
        let response = match request {};
        Ok(response)
    }
}
