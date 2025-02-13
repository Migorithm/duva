use tokio::sync::mpsc::Sender;

use crate::{
    presentation::cluster_in::communication_manager::ClusterCommunicationManager,
    services::{
        cluster::actors::commands::ClusterCommand,
        config::manager::ConfigManager,
        statefuls::cache::ttl::{actor::TtlActor, manager::TtlSchedulerManager},
    },
    CacheManager,
};

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) ttl_manager: TtlSchedulerManager,
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_actor_handler: Sender<ClusterCommand>,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}

impl ActorRegistry {
    pub(crate) fn new(
        config_manager: ConfigManager,
        cluster_actor_handler: Sender<ClusterCommand>,
    ) -> Self {
        let cache_manager = CacheManager::run_cache_actors();

        // TODO decide: do we have to run ttl actor on replica?
        let ttl_manager = TtlActor(cache_manager.clone()).run();

        Self {
            ttl_manager,
            cache_manager,
            config_manager,
            cluster_communication_manager: ClusterCommunicationManager(
                cluster_actor_handler.clone(),
            ),
            cluster_actor_handler,
        }
    }
}
