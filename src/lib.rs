pub mod adapters;
pub mod macros;
pub mod services;

use std::time::Duration;

use crate::adapters::io::tokio_stream::TokioStreamListenerFactory;
use anyhow::Result;
use services::cluster::actor::ClusterActor;
use services::cluster::inbound_mode::InboundStream;
use services::cluster::manager::ClusterManager;
use services::cluster::outbound_mode::OutboundStream;
use services::config::manager::ConfigManager;

use services::config::{ConfigResource, ConfigResponse};
use services::statefuls::cache::manager::CacheManager;
use services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use services::statefuls::persist::actor::PersistActor;
use services::stream_manager::error::IoError;
use services::stream_manager::interface::TCancellationTokenFactory;
use services::stream_manager::request_controller::client::ClientRequestController;
use services::stream_manager::{ClientStream, ClientStreamManager};
use tokio::net::TcpStream;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade<V>
where
    V: TCancellationTokenFactory,
{
    cancellation_factory: V,
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager,
    client_request_controller: &'static ClientRequestController,
    config_manager: ConfigManager,
    cluster_manager: &'static ClusterManager,
}

impl<V> StartUpFacade<V>
where
    V: TCancellationTokenFactory,
{
    pub fn new(
        cancellation_factory: V,
        config_manager: ConfigManager,
        cluster_actor: ClusterActor,
    ) -> Self {
        let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();
        let cluster_manager: &'static ClusterManager =
            Box::leak(ClusterManager::run(cluster_actor).into());

        // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
        // will live for the entire duration of the program.
        let cache_manager: &'static CacheManager = Box::leak(cache_manager.into());
        let client_request_controller: &'static ClientRequestController = Box::leak(
            ClientRequestController::new(config_manager.clone(), cache_manager, ttl_inbox.clone())
                .into(),
        );

        StartUpFacade {
            cancellation_factory,
            cache_manager,
            ttl_inbox,
            client_request_controller,
            config_manager,
            cluster_manager,
        }
    }

    // TODO: remove input config and use config manager
    pub async fn run(&mut self, startup_notifier: impl TNotifyStartUp) -> Result<()> {
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

        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.cluster_manager,
        ));
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            startup_notifier.notify_startup()
        });
        self.start_mode_specific_connection_handling().await
    }

    async fn start_accepting_peer_connections(
        peer_bind_addr: String,
        cluster_manager: &'static ClusterManager,
    ) {
        let peer_listener = TokioStreamListenerFactory
            .create_listner(&peer_bind_addr)
            .await;
        println!("Starting to accept peer connections");
        println!("listening on {}...", peer_bind_addr);

        loop {
            match peer_listener.accept().await {
                // ? how do we know if incoming connection is from a peer or replica?
                Ok((peer_stream, _socket_addr)) => {
                    tokio::spawn(cluster_manager.accept_peer(InboundStream(peer_stream)));
                }

                Err(err) => {
                    if Into::<IoError>::into(err.kind()).should_break() {
                        break;
                    }
                }
            }
        }
    }

    async fn start_accepting_client_connections(&self, bind_addr: String) {
        // SAFETY: The client_request_controller is leaked to make it static.
        // This is safe because the client_request_controller will live for the entire duration of the program.
        let client_stream_listener = TokioStreamListenerFactory.create_listner(&bind_addr).await;
        let mut conn_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(100);

        loop {
            match client_stream_listener.accept().await {
                Ok((stream, _)) =>
                // Spawn a new task to handle the connection without blocking the main thread.
                {
                    let query_manager = ClientStreamManager::new(
                        ClientStream(stream),
                        self.client_request_controller,
                    );
                    conn_handlers.push(tokio::spawn(
                        query_manager.handle_single_client_stream::<tokio::fs::File>(
                            self.cancellation_factory,
                        ),
                    ));
                }
                Err(e) => {
                    if Into::<IoError>::into(e.kind()).should_break() {
                        break;
                    }
                }
            }
        }
    }

    async fn start_mode_specific_connection_handling(&mut self) -> anyhow::Result<()> {
        loop {
            // TODO Following must be changed
            let is_master_mode = *self.config_manager.cluster_mode_watcher.borrow_and_update();
            if is_master_mode {
                self.start_accepting_client_connections(self.config_manager.bind_addr())
                    .await;
            } else {
                let ConfigResponse::ReplicationInfo(repl_info) = self
                    .config_manager
                    .route_query(ConfigResource::ReplicationInfo)
                    .await?
                else {
                    unreachable!()
                };
                self.cluster_manager
                    .join_peer(
                        OutboundStream(
                            TcpStream::connect(repl_info.master_cluster_bind_addr()).await?,
                        ),
                        repl_info,
                        self.config_manager.port,
                    )
                    .await;
            }
        }
    }
}

pub trait TNotifyStartUp: Send + 'static {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}
