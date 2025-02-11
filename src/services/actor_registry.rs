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
    pub(crate) fn new(
        ttl_manager: TtlSchedulerManager,
        cache_manager: CacheManager,
        config_manager: ConfigManager,
        cluster_manager: ClusterManager,
    ) -> Self {
        Self { ttl_manager, cache_manager, config_manager, cluster_manager }
    }
}
