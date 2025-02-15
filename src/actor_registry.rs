use crate::presentation::cluster_in::connection_manager::ClusterConnectionManager;
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;
use crate::{
    presentation::cluster_in::communication_manager::ClusterCommunicationManager,
    services::{
        cluster::actors::commands::ClusterCommand, config::manager::ConfigManager,
        statefuls::cache::ttl::manager::TtlSchedulerManager,
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
    pub(crate) fn cluster_communication_manager(&self) -> ClusterCommunicationManager {
        ClusterCommunicationManager(self.cluster_actor_handler.clone())
    }

    pub(crate) fn cluster_connection_manager(&self) -> ClusterConnectionManager {
        ClusterConnectionManager(self.cluster_communication_manager())
    }
}
