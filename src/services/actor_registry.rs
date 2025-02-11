use crate::CacheManager;

use super::{
    cluster::manager::ClusterManager, config::manager::ConfigManager,
    statefuls::cache::ttl::manager::TtlSchedulerManager,
};

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) ttl_manager: TtlSchedulerManager,
    pub(crate) cache_manager: CacheManager,

    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_manager: ClusterManager,
}

impl ActorRegistry {
    pub(crate) fn new(config_manager: ConfigManager, cluster_manager: ClusterManager) -> Self {
        let (cache_manager, ttl_manager) = CacheManager::run_cache_actors();

        Self { ttl_manager, cache_manager, config_manager, cluster_manager }
    }
}
