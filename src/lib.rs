mod actor_registry;
pub mod adapters;
mod init;
pub mod macros;
pub mod presentation;
pub mod services;
use actor_registry::ActorRegistry;
use anyhow::Result;
pub use init::Environment;
use presentation::client_in::manager::ClientManager;
use presentation::cluster_in::communication_manager::ClusterCommunicationManager;

use presentation::cluster_in::inbound::stream::InboundStream;
use services::cluster::actors::commands::ClusterCommand;
use services::cluster::replications::replication::IS_MASTER_MODE;
use services::config::manager::ConfigManager;
use services::error::IoError;
use services::statefuls::cache::manager::CacheManager;
use services::statefuls::snapshot::snapshot_loader::SnapshotLoader;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;

pub mod client_utils;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade {
    client_manager: ClientManager,
    registry: ActorRegistry,
    mode_change_watcher: tokio::sync::watch::Receiver<bool>,
}
make_smart_pointer!(StartUpFacade, ActorRegistry => registry);

impl StartUpFacade {
    pub fn new(config_manager: ConfigManager, env: Environment) -> Self {
        let (notifier, mode_change_watcher) =
            tokio::sync::watch::channel(IS_MASTER_MODE.load(Ordering::Acquire));

        let cluster_actor_handler = ClusterCommunicationManager::run(
            notifier,
            env.ttl_mills,
            env.hf_mills,
            env.init_replication_info(),
        );
        let registry = ActorRegistry::new(config_manager, cluster_actor_handler);
        let client_manager = ClientManager::new(registry.clone());
        StartUpFacade { client_manager, registry, mode_change_watcher }
    }

    pub async fn run(self, startup_notifier: impl TNotifyStartUp) -> Result<()> {
        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.registry.clone(),
        ));

        tokio::spawn(Self::initialize_with_snapshot(self.registry.clone(), startup_notifier));

        self.start_mode_specific_connection_handling().await
    }

    async fn start_accepting_peer_connections(
        peer_bind_addr: String,
        registry: ActorRegistry,
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
                    tokio::spawn({
                        let cache_m = registry.cache_manager.clone();
                        let snapshot_applier = registry.snapshot_applier.clone();
                        let inbound_stream = InboundStream::new(
                            peer_stream,
                            registry.cluster_communication_manager().replication_info().await?,
                        );

                        let connection_manager = registry.cluster_connection_manager();

                        async move {
                            if let Err(err) = connection_manager
                                .accept_inbound_stream(inbound_stream, cache_m, snapshot_applier)
                                .await
                            {
                                println!("[ERROR] Failed to accept peer connection: {:?}", err);
                            }
                        }
                    });
                }

                Err(err) => {
                    if Into::<IoError>::into(err.kind()).should_break() {
                        break Ok(());
                    }
                }
            }
        }
    }

    async fn start_mode_specific_connection_handling(mut self) -> anyhow::Result<()> {
        let mut is_master_mode = self.cluster_mode();

        loop {
            let (stop_sentinel_tx, stop_sentinel_recv) = tokio::sync::oneshot::channel::<()>();

            if is_master_mode {
                let client_stream_listener =
                    TcpListener::bind(&self.config_manager.bind_addr()).await?;

                tokio::spawn(
                    self.client_manager
                        .clone()
                        .accept_client_connections(stop_sentinel_recv, client_stream_listener),
                );

                sleep(Duration::from_millis(2)).await;
            } else {
                // Cancel all client connections only IF the cluster mode has changes to slave
                let _ = stop_sentinel_tx.send(());

                let connection_manager = self.registry.cluster_connection_manager();

                let peer_identifier = self
                    .registry
                    .cluster_communication_manager()
                    .replication_info()
                    .await?
                    .master_bind_addr();

                tokio::spawn({
                    connection_manager.discover_cluster(
                        self.config_manager.port,
                        peer_identifier,
                        self.snapshot_applier.clone(),
                    )
                });
            }

            self.wait_until_cluster_mode_changed().await?;

            is_master_mode = self.cluster_mode();
        }
    }

    // Park the task until the cluster mode changes - error means notifier has been dropped
    async fn wait_until_cluster_mode_changed(&mut self) -> anyhow::Result<()> {
        self.mode_change_watcher.changed().await?;
        Ok(())
    }
    fn cluster_mode(&mut self) -> bool {
        *self.mode_change_watcher.borrow_and_update()
    }

    async fn initialize_with_snapshot(
        registry: ActorRegistry,
        startup_notifier: impl TNotifyStartUp,
    ) -> Result<()> {
        if let Some(filepath) = registry.config_manager.try_filepath().await? {
            let snapshot = SnapshotLoader::load_from_filepath(filepath).await?;
            if let Some((repl_id, offset)) = snapshot.extract_replication_info() {
                //  TODO reconnect! - echo
                registry
                    .cluster_actor_handler
                    .send(ClusterCommand::SetReplicationInfo { master_repl_id: repl_id, offset })
                    .await?;
            };
            registry
                .cache_manager
                .apply_snapshot(
                    snapshot,
                    registry.ttl_manager,
                    registry.config_manager.startup_time,
                )
                .await?;
        }

        startup_notifier.notify_startup();
        Ok(())
    }
}

pub trait TNotifyStartUp: Send + 'static {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}
