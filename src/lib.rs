pub mod adapters;
pub mod macros;
pub mod services;
use crate::services::stream_manager::client_request_controllers::ClientRequestController;
use anyhow::Result;
use services::cluster::ClusterManager;
use services::config::config_manager::ConfigManager;

use services::statefuls::cache::cache_manager::CacheManager;
use services::statefuls::cache::ttl_manager::TtlSchedulerInbox;
use services::statefuls::persist::persist_actor::PersistActor;
use services::stream_manager::client_request_controllers::arguments::ClientRequestArguments;
use services::stream_manager::client_request_controllers::client_request::ClientRequest;
use services::stream_manager::interface::{
    TCancellationTokenFactory, TConnectStreamFactory, TExtractQuery, TStream, TStreamListener,
    TStreamListenerFactory,
};
use services::stream_manager::replication_request_controllers::arguments::PeerRequestArguments;
use services::stream_manager::replication_request_controllers::replication_request::HandShakeRequest;
use services::stream_manager::replication_request_controllers::ReplicationRequestController;
use services::stream_manager::StreamManager;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade<T, U, V, S>
where
    U: TStreamListenerFactory<S>,
    V: TCancellationTokenFactory,
    S: TStream + TExtractQuery<ClientRequest, ClientRequestArguments>,
{
    connect_stream_factory: T,
    stream_listener: U,
    cancellation_factory: V,
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager,
    client_request_controller: &'static ClientRequestController,
    replication_request_controller: &'static ReplicationRequestController,
    config_manager: ConfigManager,
    cluster_manager: &'static ClusterManager<S>,
}

impl<T, U, V, S> StartUpFacade<T, U, V, S>
where
    T: TConnectStreamFactory<S>,
    U: TStreamListenerFactory<S>,
    V: TCancellationTokenFactory,
    S: TStream
        + TExtractQuery<ClientRequest, ClientRequestArguments>
        + TExtractQuery<HandShakeRequest, PeerRequestArguments>,
{
    pub fn new(
        connect_stream_factory: T,
        stream_listener: U,
        cancellation_factory: V,
        config_manager: ConfigManager,
    ) -> Self {
        let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();
        let cluster_manager: &'static ClusterManager<S> = Box::leak(ClusterManager::run().into());

        // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
        // will live for the entire duration of the program.
        let cache_manager: &'static CacheManager = Box::leak(cache_manager.into());
        let client_request_controller: &'static ClientRequestController = Box::leak(
            ClientRequestController::new(config_manager.clone(), cache_manager, ttl_inbox.clone())
                .into(),
        );
        let replication_request_controller: &'static ReplicationRequestController =
            Box::leak(ReplicationRequestController::new(config_manager.clone()).into());

        StartUpFacade {
            connect_stream_factory,
            stream_listener,
            cancellation_factory,
            cache_manager,
            ttl_inbox,
            client_request_controller,
            replication_request_controller,
            config_manager,
            cluster_manager,
        }
    }

    // TODO: remove input config and use config manager
    pub async fn run(&self, startup_notifier: impl TNotifyStartUp) -> Result<()> {
        if let Some(filepath) = self.config_manager.try_filepath().await? {
            let dump = PersistActor::dump(filepath).await?;
            self.cache_manager
                .dump_cache(
                    dump,
                    self.ttl_inbox.clone(),
                    self.config_manager.startup_time,
                )
                .await?;
        }

        self.start_accepting_peer_connections(self.config_manager.peer_bind_addr())
            .await;

        self.start_accepting_client_connections(self.config_manager.bind_addr(), startup_notifier)
            .await;
        Ok(())
    }

    async fn start_accepting_peer_connections(&self, peer_bind_addr: String) {
        let peer_listener = self.stream_listener.create_listner(peer_bind_addr).await;
        let connect_stream_factory = self.connect_stream_factory;
        let replication_request_controller = self.replication_request_controller;

        tokio::spawn(async move {
            loop {
                match peer_listener.listen().await {
                    // ? how do we know if incoming connection is from a peer or replica?
                    Ok((peer_stream, _socket_addr)) => {
                        let query_manager =
                            StreamManager::new(peer_stream, replication_request_controller);

                        tokio::spawn(query_manager.handle_peer_stream(connect_stream_factory));
                    }

                    Err(err) => {
                        if err.should_break() {
                            break;
                        }
                    }
                }
            }
        });
    }

    async fn start_accepting_client_connections(
        &self,
        bind_addr: String,
        startup_notifier: impl TNotifyStartUp,
    ) {
        // SAFETY: The client_request_controller is leaked to make it static.
        // This is safe because the client_request_controller will live for the entire duration of the program.
        let client_stream_listener = self.stream_listener.create_listner(bind_addr).await;
        startup_notifier.notify_startup();
        loop {
            match client_stream_listener.listen().await {
                Ok((stream, _)) =>
                // Spawn a new task to handle the connection without blocking the main thread.
                {
                    let query_manager = StreamManager::new(stream, self.client_request_controller);
                    tokio::spawn(
                        query_manager.handle_single_client_stream::<tokio::fs::File>(
                            self.cancellation_factory,
                        ),
                    );
                }
                Err(e) => {
                    if e.should_break() {
                        break;
                    }
                }
            }
        }
    }
}

pub trait TNotifyStartUp {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}
