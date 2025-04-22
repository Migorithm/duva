use crate::domains::config_actors::config_manager::ConfigManager;

use crate::{
    CacheManager, presentation::clusters::communication_manager::ClusterCommunicationManager,
};

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}
