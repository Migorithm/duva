use crate::services::config::config_manager::ConfigManager;

pub mod replication_request;

pub struct ReplicationRequestController {
    config_manager: ConfigManager,
}

impl ReplicationRequestController {
    pub fn new(config_manager: ConfigManager) -> Self {
        Self { config_manager }
    }
    pub async fn handle(&self) {
        // handle replication request
    }
}
