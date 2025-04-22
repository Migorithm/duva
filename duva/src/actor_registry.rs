use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::config_actors::config_manager::ConfigManager;

use crate::{
    CacheManager, presentation::clusters::communication_manager::ClusterCommunicationManager,
};
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_actor_handler: Sender<ClusterCommand>,
}

impl ActorRegistry {
    pub(crate) fn cluster_communication_manager(&self) -> ClusterCommunicationManager {
        ClusterCommunicationManager(self.cluster_actor_handler.clone())
    }
}
