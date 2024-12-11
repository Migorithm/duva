pub mod adapters;
pub mod macros;
pub mod services;
use crate::services::query_manager::client_request_controllers::ClientRequestController;
use anyhow::Result;
use services::config::config_actor::Config;
use services::config::config_manager::ConfigManager;

use services::query_manager::interface::{
    TCancellationTokenFactory, TConnectStreamFactory, TStreamListener, TStreamListenerFactory,
};
use services::query_manager::replication_request_controllers::ReplicationRequestController;
use services::query_manager::QueryManager;
use services::statefuls::cache::cache_manager::CacheManager;
use services::statefuls::cache::ttl_manager::TtlSchedulerInbox;
use services::statefuls::persist::persist_actor::PersistActor;

// TODO should replica be able to receive replica traffics directly?
async fn start_accepting_peer_connections(
    connect_stream_factory: impl TConnectStreamFactory,
    replication_listener: impl TStreamListener,
    config_manager: ConfigManager,
) {
    let replication_request_controller: &'static ReplicationRequestController =
        Box::leak(ReplicationRequestController::new(config_manager.clone()).into());

    loop {
        match replication_listener.listen().await {
            Ok((peer_stream, _)) => {
                tokio::spawn(QueryManager::handle_peer_stream(
                    connect_stream_factory,
                    peer_stream,
                    replication_request_controller,
                ));
            }

            Err(err) => {
                if err.should_break() {
                    break;
                }
            }
        }
    }
}

// TODO should replica be able to receive client traffics directly?
async fn start_accepting_client_connections(
    cancellation_factory: impl TCancellationTokenFactory,
    client_stream_listener: impl TStreamListener,
    cache_manager: &'static CacheManager,
    ttl_inbox: TtlSchedulerInbox,
    config_manager: ConfigManager,
) {
    // SAFETY: The client_request_controller is leaked to make it static.
    // This is safe because the client_request_controller will live for the entire duration of the program.
    let client_request_controller: &'static ClientRequestController =
        Box::leak(ClientRequestController::new(config_manager, cache_manager, ttl_inbox).into());

    loop {
        match client_stream_listener.listen().await {
            Ok((stream, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                tokio::spawn(
                    QueryManager::handle_single_client_stream::<tokio::fs::File>(
                        cancellation_factory,
                        stream,
                        client_request_controller,
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

pub struct StartUpFacade<T, U, V, K> {
    connect_stream_factory: T,
    stream_listener: U,
    cancellation_factory: V,
    config: Config,
    startup_notifier: K,
}

impl<T, U, V, K> StartUpFacade<T, U, V, K>
where
    T: TConnectStreamFactory,
    U: TStreamListenerFactory,
    V: TCancellationTokenFactory,
    K: TNotifyStartUp,
{
    pub fn new(
        connect_stream_factory: T,
        stream_listener: U,
        cancellation_factory: V,
        config: Config,
        startup_notifier: K,
    ) -> Self {
        StartUpFacade {
            connect_stream_factory,
            stream_listener,
            cancellation_factory,
            config,
            startup_notifier,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let replication_stream_listener = self
            .stream_listener
            .create_listner(self.config.replication_bind_addr())
            .await;
        let client_stream_listener = self
            .stream_listener
            .create_listner(self.config.bind_addr())
            .await;

        let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();

        if let Some(filepath) = self.config.try_filepath().await {
            let dump = PersistActor::dump(filepath).await?;
            cache_manager
                .dump_cache(dump, ttl_inbox.clone(), self.config.startup_time)
                .await?;
        }

        // Run Replication manager
        let config_manager = ConfigManager::run_actor(self.config.clone());

        // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
        // will live for the entire duration of the program.
        let cache_manager: &'static CacheManager = Box::leak(Box::new(cache_manager));

        tokio::spawn(start_accepting_peer_connections(
            self.connect_stream_factory,
            replication_stream_listener,
            config_manager.clone(),
        ));
        self.startup_notifier.notify_startup();

        start_accepting_client_connections(
            self.cancellation_factory,
            client_stream_listener,
            cache_manager,
            ttl_inbox,
            config_manager,
        )
        .await;
        Ok(())
    }
}

pub trait TNotifyStartUp {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}
