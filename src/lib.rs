pub mod adapters;
pub mod macros;
pub mod services;
use anyhow::Result;
use services::client::manager::ClientManager;
use services::cluster::inbound::stream::InboundStream;
use services::cluster::manager::ClusterManager;
use services::config::manager::ConfigManager;
use services::error::IoError;
use services::interface::TCancellationTokenFactory;
use services::statefuls::cache::manager::CacheManager;
use services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use services::statefuls::persist::actor::PersistActor;
use std::time::Duration;
use tokio::net::TcpListener;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade<V>
where
    V: TCancellationTokenFactory,
{
    cancellation_factory: V,
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager,
    client_manager: &'static ClientManager,
    config_manager: ConfigManager,
    cluster_manager: &'static ClusterManager,
}

impl<V> StartUpFacade<V>
where
    V: TCancellationTokenFactory,
{
    pub fn new(cancellation_factory: V, config_manager: ConfigManager) -> Self {
        let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();
        let cluster_manager: &'static ClusterManager = Box::leak(ClusterManager::run().into());

        // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
        // will live for the entire duration of the program.
        let cache_manager: &'static CacheManager = Box::leak(cache_manager.into());
        let client_request_controller: &'static ClientManager = Box::leak(
            ClientManager::new(config_manager.clone(), cache_manager, ttl_inbox.clone())
                .into(),
        );

        StartUpFacade {
            cancellation_factory,
            cache_manager,
            ttl_inbox,
            client_manager: client_request_controller,
            config_manager,
            cluster_manager,
        }
    }

    pub async fn run(&mut self, startup_notifier: impl TNotifyStartUp) -> Result<()> {
        if let Some(filepath) = self.config_manager.try_filepath().await? {
            let dump = PersistActor::dump(filepath).await?;
            self.cache_manager
                .dump_cache(dump, self.ttl_inbox.clone(), self.config_manager.startup_time)
                .await?;
        }

        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.cluster_manager,
            self.config_manager.clone(),
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
        config_manager: ConfigManager,
    ) -> Result<()> {
        let peer_listener = TcpListener::bind(&peer_bind_addr)
            .await
            .expect("[ERROR] Failed to bind to peer address for listening");

        println!("Starting to accept peer connections");
        println!("listening peer connection on {}...", peer_bind_addr);

        loop {
            match peer_listener.accept().await {
                // ? how do we know if incoming connection is from a peer or replica?
                Ok((peer_stream, _socket_addr)) => {
                    tokio::spawn(cluster_manager.accept_peer(
                        InboundStream(peer_stream),
                        config_manager.replication_info().await?.master_replid,
                    ));
                }

                Err(err) => {
                    if Into::<IoError>::into(err.kind()).should_break() {
                        break Ok(());
                    }
                }
            }
        }
    }

    async fn start_mode_specific_connection_handling(&mut self) -> anyhow::Result<()> {
        let mut is_master_mode = self.config_manager.cluster_mode();
        loop {
            let (stop_sentinel_tx, stop_sentinel_recv) = tokio::sync::oneshot::channel::<()>();

            if is_master_mode {
                let client_stream_listener =
                    TcpListener::bind(&self.config_manager.bind_addr()).await?;
                tokio::spawn(self.client_manager.receive_clients(
                    self.cancellation_factory,
                    stop_sentinel_recv,
                    client_stream_listener,
                ));
            } else {
                // Cancel all client connections only IF the cluster mode has changes to slave
                let _ = stop_sentinel_tx.send(());
                let repl_info = self.config_manager.replication_info().await?;
                tokio::spawn(
                    self.cluster_manager.discover_cluster(repl_info, self.config_manager.port),
                );
            }

            self.config_manager.wait_until_cluster_mode_changed().await?;
            is_master_mode = self.config_manager.cluster_mode();
        }
    }
}

pub trait TNotifyStartUp: Send + 'static {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}
