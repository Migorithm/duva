mod actor_registry;
pub mod adapters;
pub mod domains;
mod init;
pub mod macros;
pub mod presentation;
pub mod services;
use actor_registry::ActorRegistry;
use anyhow::Result;
use domains::IoError;
use domains::append_only_files::interfaces::TWriteAheadLog;
use domains::caches::cache_manager::CacheManager;
use domains::cluster_actors::ClusterActor;
use domains::cluster_actors::commands::ClusterCommand;
use domains::cluster_actors::replication::{IS_LEADER_MODE, ReplicationInfo};
use domains::config_actors::config_manager::ConfigManager;
use domains::saves::snapshot::snapshot_applier::SnapshotApplier;
use domains::saves::snapshot::snapshot_loader::SnapshotLoader;
pub use init::Environment;
use presentation::clients::ClientController;
use presentation::cluster_in::inbound::stream::InboundStream;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;

pub mod client_utils;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade {
    registry: ActorRegistry,
    mode_change_watcher: tokio::sync::watch::Receiver<bool>,
}
make_smart_pointer!(StartUpFacade, ActorRegistry => registry);

impl StartUpFacade {
    pub fn new(config_manager: ConfigManager, env: Environment, wal: impl TWriteAheadLog) -> Self {
        let (notifier, mode_change_watcher) =
            tokio::sync::watch::channel(IS_LEADER_MODE.load(Ordering::Acquire));

        let cache_manager = CacheManager::run_cache_actors();
        let cluster_actor_handler = ClusterActor::run(
            env.ttl_mills,
            env.hf_mills,
            ReplicationInfo::new(env.replicaof, &env.host, env.port),
            cache_manager.clone(),
            notifier,
            wal,
        );

        let registry = ActorRegistry {
            snapshot_applier: SnapshotApplier::new(
                cache_manager.clone(),
                config_manager.startup_time,
            ),
            cluster_actor_handler,
            config_manager,
            cache_manager,
        };

        StartUpFacade { registry, mode_change_watcher }
    }

    pub async fn run(self) -> Result<()> {
        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.registry.clone(),
        ));

        self.initialize_with_snapshot().await?;
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
                        let current_repo_info =
                            registry.cluster_communication_manager().replication_info().await?;

                        let inbound_stream = InboundStream::new(peer_stream, current_repo_info);

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
                },

                Err(err) => {
                    if Into::<IoError>::into(err.kind()).should_break() {
                        break Ok(());
                    }
                },
            }
        }
    }

    async fn start_mode_specific_connection_handling(mut self) -> anyhow::Result<()> {
        let mut is_leader_mode = self.cluster_mode();

        loop {
            let (stop_sentinel_tx, stop_sentinel_recv) = tokio::sync::oneshot::channel::<()>();

            if is_leader_mode {
                println!("[INFO] Run on leader mode. Start accepting client stream...");
                let client_stream_listener =
                    TcpListener::bind(&self.config_manager.bind_addr()).await?;

                tokio::spawn(
                    ClientController::new(self.registry.clone())
                        .accept_client_connections(stop_sentinel_recv, client_stream_listener),
                );

                sleep(Duration::from_millis(2)).await;
            } else {
                // Cancel all client connections only IF the cluster mode has changes to follower
                let _ = stop_sentinel_tx.send(());

                let connection_manager = self.registry.cluster_connection_manager();

                let peer_identifier = self
                    .registry
                    .cluster_communication_manager()
                    .replication_info()
                    .await?
                    .leader_bind_addr();

                tokio::spawn({
                    connection_manager.discover_cluster(
                        self.config_manager.port,
                        peer_identifier,
                        self.snapshot_applier.clone(),
                    )
                });
            }

            self.wait_until_cluster_mode_changed().await?;

            is_leader_mode = self.cluster_mode();
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

    async fn initialize_with_snapshot(&self) -> Result<()> {
        if let Some(filepath) = self.registry.config_manager.try_filepath().await? {
            let snapshot = SnapshotLoader::load_from_filepath(filepath).await?;
            let (repl_id, hwm) = snapshot.extract_replication_info();
            // Reconnection case - set the replication info
            self.registry
                .cluster_actor_handler
                .send(ClusterCommand::SetReplicationInfo { leader_repl_id: repl_id, hwm })
                .await?;
            self.registry.snapshot_applier.apply_snapshot(snapshot).await?;
        }
        Ok(())
    }
}
