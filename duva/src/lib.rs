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
use domains::cluster_actors::replication::ReplicationRole;
use domains::cluster_actors::replication::ReplicationState;
use domains::config_actors::config_manager::ConfigManager;
use domains::saves::snapshot::snapshot_loader::SnapshotLoader;
pub use init::Environment;
use prelude::PeerIdentifier;
use presentation::clients::ClientController;
use presentation::clients::authenticate;

use presentation::clusters::inbound::stream::InboundStream;

use tokio::net::TcpListener;

pub mod prelude {
    pub use crate::domains::peers::identifier::PeerIdentifier;
    pub use anyhow;
    pub use bytes;
    pub use bytes::BytesMut;
    pub use tokio;
    pub use uuid;
}

pub mod clients;

// * StartUp Facade that manages invokes subsystems
pub struct StartUpFacade {
    registry: ActorRegistry,
}
make_smart_pointer!(StartUpFacade, ActorRegistry => registry);

impl StartUpFacade {
    pub fn new(
        config_manager: ConfigManager,
        env: &mut Environment,
        wal: impl TWriteAheadLog,
    ) -> Self {
        let replication_state =
            ReplicationState::new(env.repl_id.clone(), env.role.clone(), &env.host, env.port);
        let cache_manager = CacheManager::run_cache_actors(replication_state.hwm.clone());
        let cluster_actor_handler = ClusterActor::run(
            env.ttl_mills,
            env.topology_writer.take().unwrap(),
            env.hf_mills,
            replication_state,
            cache_manager.clone(),
            wal,
        );

        let registry = ActorRegistry { cluster_actor_handler, config_manager, cache_manager };

        StartUpFacade { registry }
    }

    pub async fn run(self, env: Environment) -> Result<()> {
        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.registry.clone(),
        ));

        self.initialize_with_snapshot().await?;
        if let Some(seed_server) = env.seed_server {
            self.registry.cluster_communication_manager().discover_cluster(seed_server).await?;
        } else {
            // TODO reconnection failure? - if all fail, the server should be leader?

            for pre_connected in env.pre_connected_peers {
                if let Ok(()) = self
                    .registry
                    .cluster_communication_manager()
                    .discover_cluster(pre_connected.bind_addr)
                    .await
                {
                    break;
                }
            }
        }
        self.start_receiving_client_streams().await
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
                        let ccm = registry.cluster_communication_manager();
                        let current_repo_info = ccm.replication_info().await?;

                        let inbound_stream = InboundStream::new(peer_stream, current_repo_info);

                        let connection_manager = registry.cluster_connection_manager();

                        async move {
                            if let Err(err) =
                                connection_manager.accept_inbound_stream(inbound_stream, ccm).await
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

    /// Run while loop accepting stream and if the sentinel is received, abort the tasks
    async fn start_receiving_client_streams(self) -> anyhow::Result<()> {
        let client_stream_listener = TcpListener::bind(&self.config_manager.bind_addr()).await?;
        println!("start listening on {}", self.config_manager.bind_addr());
        let mut conn_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(100);

        while let Ok((stream, _)) = client_stream_listener.accept().await {
            let mut peers = self.registry.cluster_communication_manager().get_peers().await?;
            peers.push(PeerIdentifier(self.registry.config_manager.bind_addr()));

            let is_leader = self.registry.cluster_communication_manager().role().await?
                == ReplicationRole::Leader;
            let Ok((reader, writer)) = authenticate(stream, peers, is_leader).await else {
                eprintln!("[ERROR] Failed to authenticate client stream");
                continue;
            };

            let topology_observer =
                self.registry.cluster_communication_manager().subscribe_topology_change().await?;
            let write_handler = writer.run(topology_observer);

            conn_handlers.push(tokio::spawn(reader.handle_client_stream(
                ClientController::new(self.registry.clone()),
                write_handler.clone(),
            )));
        }

        Ok(())
    }

    async fn initialize_with_snapshot(&self) -> Result<()> {
        if let Some(filepath) = self.registry.config_manager.try_filepath().await? {
            let snapshot = SnapshotLoader::load_from_filepath(filepath).await?;
            let (repl_id, hwm) = snapshot.extract_replication_info();
            // Reconnection case - set the replication info
            self.registry
                .cluster_actor_handler
                .send(ClusterCommand::SetReplicationInfo { replid: repl_id, hwm })
                .await?;
            self.registry.cache_manager.apply_snapshot(snapshot).await?;
        }
        Ok(())
    }
}
