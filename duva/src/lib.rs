pub mod adapters;
mod config;
pub mod domains;
pub mod macros;
pub mod presentation;
mod types;
use anyhow::Result;
pub use config::Environment;
use domains::IoError;
use domains::caches::cache_manager::CacheManager;
use domains::cluster_actors::ClusterActor;
use domains::cluster_actors::ConnectionMessage;
use domains::cluster_actors::replication::ReplicationId;

use domains::cluster_actors::replication::ReplicationState;
use domains::operation_logs::interfaces::TWriteAheadLog;
use domains::saves::snapshot::Snapshot;
use domains::saves::snapshot::snapshot_loader::SnapshotLoader;
use presentation::clients::ClientController;
use presentation::clients::authenticate;
use presentation::clusters::communication_manager::ClusterCommunicationManager;
use std::fs::File;
use tokio::net::TcpListener;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use uuid::Uuid;

pub use config::ENV;
pub mod prelude {
    pub use crate::domains::cluster_actors::actor::heartbeat_scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;
    pub use crate::domains::cluster_actors::topology::NodeReplInfo;
    pub use crate::domains::cluster_actors::topology::Topology;
    pub use crate::domains::peers::identifier::PeerIdentifier;
    pub use crate::presentation::clients::AuthRequest;
    pub use crate::presentation::clients::AuthResponse;
    pub use anyhow;
    pub use bytes;
    pub use bytes::BytesMut;
    pub use rand;
    pub use tokio;
    pub use uuid;
}

// * StartUp Facade that manages invokes subsystems
#[derive(Clone)]
pub struct StartUpFacade {
    cluster_communication_manager: ClusterCommunicationManager,
    cache_manager: CacheManager,
}

impl StartUpFacade {
    // Refactiring : this should run before cluster actor runs
    fn initialize_with_snapshot() -> Snapshot {
        let path_str = format!("{}/{}", ENV.dir, ENV.dbfilename);
        let path = std::path::Path::new(path_str.as_str());

        // todo if tpp was modified AFTER snapshot was created, we need to update the repl id
        let repl_id_from_topp = if ENV.seed_server.is_none() {
            ReplicationId::Key(
                ENV.stored_peer_states
                    .iter()
                    .find(|p| p.is_self(ENV.bind_addr().as_str()))
                    .map(|p| p.replid.to_string())
                    .unwrap_or_else(|| Uuid::now_v7().to_string()),
            )
        } else {
            ReplicationId::Undecided
        };

        if let Ok(true) = path.try_exists()
            && let Ok(snapshot) = SnapshotLoader::load_from_filepath(path)
        {
            return snapshot;
        }

        Snapshot::default_with_repl_id(repl_id_from_topp)
    }

    pub fn new(wal: impl TWriteAheadLog, writer: File) -> Self {
        let snapshot_info = Self::initialize_with_snapshot();
        let (r_id, hwm) = snapshot_info.extract_replication_info();

        let replication_state =
            ReplicationState::new(r_id, ENV.role.clone(), &ENV.host, ENV.port, hwm);
        let cache_manager = CacheManager::run_cache_actors(replication_state.hwm.clone());
        tokio::spawn(cache_manager.clone().apply_snapshot(snapshot_info.key_values()));

        let cluster_actor_handler = ClusterActor::run(
            ENV.ttl_mills,
            writer,
            ENV.hf_mills,
            replication_state,
            cache_manager.clone(),
            wal,
        );

        StartUpFacade {
            cluster_communication_manager: ClusterCommunicationManager(cluster_actor_handler),

            cache_manager,
        }
    }

    pub async fn run(self) -> Result<()> {
        tokio::spawn(Self::start_accepting_peer_connections(
            ENV.peer_bind_addr(),
            self.cluster_communication_manager.clone(),
        ));

        self.discover_cluster().await?;
        self.start_receiving_client_streams().await
    }

    async fn discover_cluster(&self) -> Result<(), anyhow::Error> {
        if let Some(seed) = ENV.seed_server.as_ref() {
            return self.cluster_communication_manager.route_connect_to_server(seed.clone()).await;
        }

        for peer in ENV.stored_peer_states.iter().filter(|p| !p.is_self(&ENV.bind_addr())) {
            if let Err(err) =
                self.cluster_communication_manager.route_connect_to_server(peer.id().clone()).await
            {
                error!("{err}");
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn start_accepting_peer_connections(
        peer_bind_addr: String,
        cluster_communication_manager: ClusterCommunicationManager,
    ) -> Result<()> {
        let peer_listener = TcpListener::bind(&peer_bind_addr).await.unwrap();

        info!("listening peer connection on {}...", peer_bind_addr);
        loop {
            match peer_listener.accept().await {
                | Ok((peer_stream, socket_addr)) => {
                    debug!("Accepted peer connection: {}", socket_addr);
                    let (read, write) = peer_stream.into_split();
                    let host_ip = socket_addr.ip().to_string();
                    if cluster_communication_manager
                        .send(ConnectionMessage::AcceptInboundPeer {
                            read: read.into(),
                            write: write.into(),
                            host_ip,
                        })
                        .await
                        .is_err()
                    {
                        error!("Failed to accept peer connection");
                    }
                },

                | Err(err) => {
                    error!("Failed to accept peer connection: {:?}", err);
                    if Into::<IoError>::into(err.kind()).should_break() {
                        break Ok(());
                    }
                },
            }
        }
    }

    /// Run while loop accepting stream and if the sentinel is received, abort the tasks

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    async fn start_receiving_client_streams(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(ENV.bind_addr()).await?;
        info!("start listening on {}", ENV.bind_addr());
        let mut handles = Vec::with_capacity(100);

        //TODO refactor: authentication should be simplified
        while let Ok((stream, _)) = listener.accept().await {
            let Ok((reader, writer)) =
                authenticate(stream, &self.cluster_communication_manager).await
            else {
                error!("Failed to authenticate client stream");
                continue;
            };

            let observer =
                self.cluster_communication_manager.route_subscribe_topology_change().await?;
            let write_handler = writer.run(observer);

            handles.push(tokio::spawn(
                reader.handle_client_stream(self.client_controller(), write_handler.clone()),
            ));
        }

        Ok(())
    }

    pub(crate) fn client_controller(&self) -> ClientController {
        ClientController {
            cluster_communication_manager: self.cluster_communication_manager.clone(),
            cache_manager: self.cache_manager.clone(),
        }
    }
}
