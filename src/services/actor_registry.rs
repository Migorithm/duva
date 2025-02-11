use crate::CacheManager;

use super::{
    cluster::manager::ClusterManager, config::manager::ConfigManager,
    statefuls::cache::ttl::manager::TtlSchedulerManager,
};

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) ttl_inbox: TtlSchedulerManager,
    pub(crate) cache_manager: CacheManager,

    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_manager: ClusterManager,
}
