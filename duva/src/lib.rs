pub mod adapters;
pub mod domains;
mod init;
pub mod macros;
pub mod presentation;
use anyhow::Result;
use domains::IoError;
use domains::caches::cache_manager::CacheManager;
use domains::cluster_actors::ClusterActor;
use domains::cluster_actors::ConnectionMessage;
use domains::cluster_actors::replication::ReplicationId;
use domains::cluster_actors::replication::ReplicationRole;
use domains::cluster_actors::replication::ReplicationState;
use domains::config_actors::config_manager::ConfigManager;
use domains::operation_logs::interfaces::TWriteAheadLog;
use domains::peers::peer::NodeKind;
use domains::saves::snapshot::Snapshot;
use domains::saves::snapshot::snapshot_loader::SnapshotLoader;
pub use init::Environment;
use prelude::PeerIdentifier;
use presentation::clients::ClientController;
use presentation::clients::authenticate;

use presentation::clusters::communication_manager::ClusterCommunicationManager;

use tokio::net::TcpListener;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use uuid::Uuid;

pub mod prelude {
    pub use crate::domains::cluster_actors::heartbeat_scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;
    pub use crate::domains::peers::identifier::PeerIdentifier;
    pub use crate::presentation::clients::AuthRequest;
    pub use crate::presentation::clients::AuthResponse;
    pub use anyhow;
    pub use bytes;
    pub use bytes::BytesMut;
    pub use tokio;
    pub use uuid;
}

// * StartUp Facade that manages invokes subsystems
#[derive(Clone)]
pub struct StartUpFacade {
    cluster_communication_manager: ClusterCommunicationManager,
    config_manager: ConfigManager,
    cache_manager: CacheManager,
}

impl StartUpFacade {
    // Refactiring : this should run before cluster actor runs
    fn initialize_with_snapshot(env: &Environment) -> Snapshot {
        let path_str = format!("{}/{}", env.dir, env.dbfilename);
        let path = std::path::Path::new(path_str.as_str());

        // todo if tpp was modified AFTER snapshot was created, we need to update the repl id
        let _repl_id_from_topp = if env.seed_server.is_none() {
            ReplicationId::Key(
                env.pre_connected_peers
                    .iter()
                    .find(|p| p.kind == NodeKind::Replica)
                    .map(|p| p.replid.to_string())
                    .unwrap_or_else(|| Uuid::now_v7().to_string()),
            )
        } else {
            ReplicationId::Undecided
        };

        if let Ok(true) = path.try_exists() {
            let snapshot = SnapshotLoader::load_from_filepath(path).unwrap();

            return snapshot;
        }

        Snapshot::default()
    }

    pub fn new(
        config_manager: ConfigManager,
        env: &mut Environment,
        wal: impl TWriteAheadLog,
    ) -> Self {
        let snapshot_info = Self::initialize_with_snapshot(env);
        let (r_id, hwm) = snapshot_info.extract_replication_info();

        let replication_state =
            ReplicationState::new(r_id, env.role.clone(), &env.host, env.port, hwm);
        let cache_manager = CacheManager::run_cache_actors(replication_state.hwm.clone());
        tokio::spawn(cache_manager.clone().apply_snapshot(snapshot_info.key_values()));

        let cluster_actor_handler = ClusterActor::run(
            env.ttl_mills,
            env.topology_writer.take().unwrap(),
            env.hf_mills,
            replication_state,
            cache_manager.clone(),
            wal,
        );

        StartUpFacade {
            cluster_communication_manager: ClusterCommunicationManager(cluster_actor_handler),
            config_manager,
            cache_manager,
        }
    }

    pub async fn run(self, env: Environment) -> Result<()> {
        tokio::spawn(Self::start_accepting_peer_connections(
            self.config_manager.peer_bind_addr(),
            self.cluster_communication_manager.clone(),
        ));

        self.discover_cluster(env).await?;
        self.start_receiving_client_streams().await
    }

    async fn discover_cluster(&self, env: Environment) -> Result<(), anyhow::Error> {
        if let Some(seed) = env.seed_server {
            return self.cluster_communication_manager.connect_to_server(seed).await;
        }

        for peer in env.pre_connected_peers {
            if let Err(err) = self.cluster_communication_manager.connect_to_server(peer.addr).await
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
                // ? how do we know if incoming connection is from a peer or replica?
                | Ok((peer_stream, socket_addr)) => {
                    debug!("Accepted peer connection: {}", socket_addr);
                    if cluster_communication_manager
                        .send(ConnectionMessage::AcceptInboundPeer { stream: peer_stream })
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

    #[instrument(skip(self))]
    async fn start_receiving_client_streams(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config_manager.bind_addr()).await?;
        info!("start listening on {}", self.config_manager.bind_addr());
        let mut handles = Vec::with_capacity(100);

        //TODO refactor: authentication should be simplified
        while let Ok((stream, _)) = listener.accept().await {
            let mut peers = self.cluster_communication_manager.get_peers().await?;
            peers.push(PeerIdentifier(self.config_manager.bind_addr()));

            let is_leader: bool =
                self.cluster_communication_manager.role().await? == ReplicationRole::Leader;
            let Ok((reader, writer)) = authenticate(stream, peers, is_leader).await else {
                error!("Failed to authenticate client stream");
                continue;
            };

            let observer = self.cluster_communication_manager.subscribe_topology_change().await?;
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
            config_manager: self.config_manager.clone(),
        }
    }
}
