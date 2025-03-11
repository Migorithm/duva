use crate::actor_registry::ActorRegistry;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::config_actors::command::ConfigResponse;
use crate::domains::config_actors::config_manager::ConfigManager;
use crate::domains::query_parsers::QueryIO;
use crate::domains::saves::actor::SaveTarget;

use crate::presentation::clients::request::ClientRequest;
use crate::presentation::clients::stream::ClientStream;
use crate::presentation::cluster_in::communication_manager::ClusterCommunicationManager;
use crate::services::interface::TWrite;
use futures::future::try_join_all;
use std::marker::PhantomData;
use tokio::net::TcpStream;

pub mod acceptor;
pub mod handler;

pub(crate) struct Handler;
#[derive(Clone)]
pub(crate) struct Acceptor;

#[derive(Clone)]
pub(crate) struct ClientController<T = Acceptor> {
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
    pub(crate) acceptor: PhantomData<T>,
}
