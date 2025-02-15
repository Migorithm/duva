use crate::presentation::cluster_in::connection_manager::ClusterConnectionManager;
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;
use crate::{
    presentation::cluster_in::communication_manager::ClusterCommunicationManager,
    services::{
        cluster::actors::commands::ClusterCommand,
        config::manager::ConfigManager,
        statefuls::cache::ttl::{actor::TtlActor, manager::TtlSchedulerManager},
    },
    CacheManager,
};
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct ActorRegistry {
    pub(crate) ttl_manager: TtlSchedulerManager,
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_actor_handler: Sender<ClusterCommand>,
    pub(crate) snapshot_applier: SnapshotApplier,
}

impl ActorRegistry {
    pub(crate) fn new(
        config_manager: ConfigManager,
        cluster_actor_handler: Sender<ClusterCommand>,
    ) -> Self {
        let cache_manager = CacheManager::run_cache_actors();

        // TODO decide: do we have to run ttl actor on replica?
        let ttl_manager = TtlActor(cache_manager.clone()).run();

        let snapshot_applier = SnapshotApplier::new(
            cache_manager.clone(),
            ttl_manager.clone(),
            config_manager.startup_time,
        );

        Self {
            ttl_manager,
            cache_manager,
            config_manager,

            cluster_actor_handler: cluster_actor_handler,
            snapshot_applier,
        }
    }

    pub(crate) fn cluster_communication_manager(&self) -> ClusterCommunicationManager {
        ClusterCommunicationManager(self.cluster_actor_handler.clone())
    }

    pub(crate) fn cluster_connection_manager(&self) -> ClusterConnectionManager {
        ClusterConnectionManager(self.cluster_communication_manager())
    }
}
