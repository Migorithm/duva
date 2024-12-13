pub mod adapters;
pub mod macros;
pub mod services;
use crate::services::stream_manager::client_request_controllers::ClientRequestController;
use anyhow::Result;
use services::config::config_manager::ConfigManager;

use services::statefuls::cache::cache_manager::CacheManager;
use services::statefuls::cache::ttl_manager::TtlSchedulerInbox;
use services::statefuls::persist::persist_actor::PersistActor;
use services::stream_manager::interface::{
    TCancellationTokenFactory, TConnectStreamFactory, TStreamListener, TStreamListenerFactory,
};
use services::stream_manager::replication_request_controllers::ReplicationRequestController;
use services::stream_manager::StreamManager;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade<T, U, V> {
    connect_stream_factory: T,
    stream_listener: U,
    cancellation_factory: V,
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager,
    client_request_controller: &'static ClientRequestController,
    replication_request_controller: &'static ReplicationRequestController,
    config_manager: ConfigManager,
}

impl<T, U, V> StartUpFacade<T, U, V>
where
    T: TConnectStreamFactory,
    U: TStreamListenerFactory,
    V: TCancellationTokenFactory,
{
    pub fn new(
        connect_stream_factory: T,
        stream_listener: U,
        cancellation_factory: V,
        config_manager: ConfigManager,
    ) -> Self {
        let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();

        // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
        // will live for the entire duration of the program.
        let cache_manager: &'static CacheManager = Box::leak(Box::new(cache_manager));
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

    async fn start_accepting_peer_connections(&self, replication_bind_addr: String) {
        let replication_listener = self
            .stream_listener
            .create_listner(replication_bind_addr)
            .await;
        let connect_stream_factory = self.connect_stream_factory;
        let replication_request_controller = self.replication_request_controller;

        tokio::spawn(async move {
            loop {
                match replication_listener.listen().await {
                    Ok((peer_stream, _)) => {
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
