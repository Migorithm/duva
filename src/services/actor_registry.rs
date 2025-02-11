use super::{
    cluster::manager::ClusterManager,
    config::manager::ConfigManager,
    statefuls::cache::ttl::{actor::TtlActor, manager::TtlSchedulerManager},
};
use crate::CacheManager;

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) ttl_manager: TtlSchedulerManager,
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_manager: ClusterManager,
}

impl ActorRegistry {
    pub(crate) fn new(config_manager: ConfigManager, cluster_manager: ClusterManager) -> Self {
        let cache_manager = CacheManager::run_cache_actors();

        // TODO decide: do we have to run ttl actor on replica?
        let ttl_manager = TtlActor(cache_manager.clone()).run();

        Self { ttl_manager, cache_manager, config_manager, cluster_manager }
    }
}
