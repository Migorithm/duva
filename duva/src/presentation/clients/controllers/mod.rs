use crate::actor_registry::ActorRegistry;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::config_actors::command::ConfigResponse;
use crate::domains::config_actors::config_manager::ConfigManager;
use crate::domains::query_parsers::QueryIO;
use crate::domains::saves::actor::SaveTarget;
use crate::presentation::clients::request::ClientAction;
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;
use futures::future::try_join_all;
mod authenticate;
pub(crate) use authenticate::authenticate;
pub mod handler;

#[derive(Clone)]
pub(crate) struct ClientController {
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}

impl ClientController {
    pub(crate) fn new(actor_registry: ActorRegistry) -> Self {
        Self {
            cluster_communication_manager: actor_registry.cluster_communication_manager(),
            cache_manager: actor_registry.cache_manager,
            config_manager: actor_registry.config_manager,
        }
    }
}
