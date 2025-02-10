pub mod adapters;
pub mod macros;
pub mod services;
use anyhow::Result;
use services::client::manager::ClientManager;
use services::cluster::command::cluster_command::ClusterCommand;
use services::cluster::inbound::stream::InboundStream;
use services::cluster::manager::ClusterManager;
use services::cluster::replications::replication::IS_MASTER_MODE;
use services::config::init::get_env;
use services::config::manager::ConfigManager;
use services::error::IoError;
use services::statefuls::cache::manager::CacheManager;
use services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use services::statefuls::snapshot::dump_loader::DumpLoader;
use std::sync::atomic::Ordering;
use std::thread::sleep;
use std::time::Duration;
use tokio::net::TcpListener;

pub mod client_utils;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade {
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager,
    client_manager: &'static ClientManager,
    config_manager: ConfigManager,
    cluster_manager: &'static ClusterManager,
    mode_change_watcher: tokio::sync::watch::Receiver<bool>,
}

impl StartUpFacade {
    pub fn new(config_manager: ConfigManager) -> Self {
        let _ = get_env();
        let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();

        let (notifier, mode_change_watcher) =
            tokio::sync::watch::channel(IS_MASTER_MODE.load(Ordering::Acquire));
        let cluster_manager: &'static ClusterManager =
            Box::leak(ClusterManager::run(notifier).into());

        // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
        // will live for the entire duration of the program.
        let cache_manager: &'static CacheManager = Box::leak(cache_manager.into());
        let client_request_controller: &'static ClientManager = Box::leak(
            ClientManager::new(
                config_manager.clone(),
                cache_manager,
                cluster_manager,
                ttl_inbox.clone(),
            )
            .into(),
        );

        StartUpFacade {
            cache_manager,
            ttl_inbox,
            client_manager: client_request_controller,
            config_manager,
            cluster_manager,
            mode_change_watcher,
        }
    }

    pub async fn run(&mut self, startup_notifier: impl TNotifyStartUp) -> Result<()> {
        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.cluster_manager,
            self.cache_manager,
        ));

        tokio::spawn(Self::initialize_with_dump(
            self.config_manager.clone(),
            self.cache_manager,
            self.ttl_inbox.clone(),
            self.cluster_manager,
            startup_notifier,
        ));

        self.start_mode_specific_connection_handling().await
    }

    async fn start_accepting_peer_connections(
        peer_bind_addr: String,
        cluster_manager: &'static ClusterManager,
        cache_manager: &'static CacheManager,
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
                    let repl_info = cluster_manager.replication_info().await?;

                    tokio::spawn(async move {
                        if let Err(err) = cluster_manager
                            .accept_inbound_stream(
                                InboundStream::new(peer_stream, repl_info),
                                cache_manager,
                            )
                            .await
                        {
                            println!("[ERROR] Failed to accept peer connection: {:?}", err);
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

    async fn start_mode_specific_connection_handling(&mut self) -> anyhow::Result<()> {
        let mut is_master_mode = self.cluster_mode();

        loop {
            let (stop_sentinel_tx, stop_sentinel_recv) = tokio::sync::oneshot::channel::<()>();

            if is_master_mode {
                let client_stream_listener =
                    TcpListener::bind(&self.config_manager.bind_addr()).await?;

                tokio::spawn(
                    self.client_manager
                        .accept_client_connections(stop_sentinel_recv, client_stream_listener),
                );

                sleep(Duration::from_millis(2));
            } else {
                // Cancel all client connections only IF the cluster mode has changes to slave
                let _ = stop_sentinel_tx.send(());

                tokio::spawn(self.cluster_manager.discover_cluster(
                    self.config_manager.port,
                    self.cluster_manager.replication_info().await?.master_bind_addr(),
                ));
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

    async fn initialize_with_dump(
        config_manager: ConfigManager,
        cache_manager: &'static CacheManager,
        ttl_inbox: TtlSchedulerInbox,
        cluster_manager: &'static ClusterManager,
        startup_notifier: impl TNotifyStartUp,
    ) -> Result<()> {
        if let Some(filepath) = config_manager.try_filepath().await? {
            let dump = DumpLoader::load(filepath).await?;
            if let Some((repl_id, offset)) = dump.extract_replication_info() {
                //  TODO reconnect! - echo
                cluster_manager
                    .send(ClusterCommand::SetReplicationInfo { master_repl_id: repl_id, offset })
                    .await?;
            };
            cache_manager.dump_cache(dump, ttl_inbox.clone(), config_manager.startup_time).await?;
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
