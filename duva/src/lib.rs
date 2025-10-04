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
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use presentation::clients::ClientController;

use std::fs::File;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::util::SubscriberInitExt;

use tracing_subscriber::layer::SubscriberExt;
use uuid::Uuid;

use crate::domains::TSerdeRead;

use crate::domains::cluster_actors::queue::ClusterActorSender;

use crate::domains::operation_logs::logger::ReplicatedLogs;
use crate::prelude::ConnectionRequest;
use crate::presentation::clients::stream::ClientStreamReader;
use crate::presentation::clients::stream::ClientStreamWriter;
pub use config::ENV;
pub mod prelude {
    pub use crate::domains::cluster_actors::actor::heartbeat_scheduler::ELECTION_TIMEOUT_MAX;
    pub use crate::domains::cluster_actors::topology::Topology;
    pub use crate::domains::peers::identifier::PeerIdentifier;
    pub use crate::presentation::clients::ConnectionRequest;
    pub use crate::presentation::clients::ConnectionResponse;
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
    cluster_actor_sender: ClusterActorSender,
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
                ENV.stored_node_states
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
        let (r_id, con_idx) = snapshot_info.extract_replication_info();

        let replication_state = ReplicationState::new(
            r_id,
            ENV.role.clone(),
            &ENV.host,
            ENV.port,
            ReplicatedLogs::new(wal, con_idx, 0),
        );
        let cache_manager =
            CacheManager::run_cache_actors(replication_state.logger.con_idx.clone());
        tokio::spawn(cache_manager.clone().apply_snapshot(snapshot_info.key_values()));

        let cluster_actor_handler =
            ClusterActor::run(writer, ENV.hf_mills, replication_state, cache_manager.clone());

        StartUpFacade { cluster_actor_sender: cluster_actor_handler, cache_manager }
    }

    pub async fn run(self) -> Result<()> {
        let logger_provider = init_logs();
        // Create a new OpenTelemetryTracingBridge using the above LoggerProvider.
        let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider);

        let filter_otel = EnvFilter::new(ENV.log_level.to_string())
            .add_directive("hyper=off".parse().unwrap())
            .add_directive("tonic=off".parse().unwrap())
            .add_directive("h2=off".parse().unwrap())
            .add_directive("reqwest=off".parse().unwrap());
        let otel_layer = otel_layer.with_filter(filter_otel);
        let filter_fmt = EnvFilter::new("info").add_directive("opentelemetry=off".parse().unwrap());

        let fmt_layer = tracing_subscriber::fmt::layer().with_filter(filter_fmt);

        tracing_subscriber::registry().with(otel_layer).with(fmt_layer).init();

        tokio::spawn(Self::start_accepting_peer_connections(
            ENV.peer_bind_addr(),
            self.cluster_actor_sender.clone(),
        ));

        self.discover_cluster().await?;
        let _ = self.start_accepting_client_streams().await;

        info!("Server shut down...");
        logger_provider.shutdown().unwrap();

        Ok(())
    }

    async fn discover_cluster(&self) -> Result<(), anyhow::Error> {
        if let Some(seed) = ENV.seed_server.as_ref() {
            return self.cluster_actor_sender.route_connect_to_server(seed.clone()).await;
        }

        for node in ENV.stored_node_states.iter().filter(|p| !p.is_self(&ENV.bind_addr())) {
            if let Err(err) =
                self.cluster_actor_sender.route_connect_to_server(node.id().clone()).await
            {
                err!("{}", err);
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn start_accepting_peer_connections(
        peer_bind_addr: String,
        ca_sender: ClusterActorSender,
    ) -> Result<()> {
        let peer_listener = TcpListener::bind(&peer_bind_addr).await.unwrap();

        info!("listening peer connection on {}...", peer_bind_addr);
        loop {
            match peer_listener.accept().await {
                | Ok((peer_stream, socket_addr)) => {
                    let (read, write) = peer_stream.into_split();
                    let host_ip = socket_addr.ip().to_string();
                    if ca_sender
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

    #[instrument(level = tracing::Level::DEBUG, skip(self))]
    async fn start_accepting_client_streams(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(ENV.bind_addr()).await?;
        info!("start listening on {}", ENV.bind_addr());

        //TODO refactor: authentication should be simplified
        while let Ok((stream, _)) = listener.accept().await {
            let (mut read_half, write_half) = stream.into_split();

            let mut writer = ClientStreamWriter(write_half);
            let request = read_half.deserialized_read().await?;
            self.cluster_actor_sender.wait_for_acceptance().await;

            match request {
                | ConnectionRequest { .. } => {
                    let Ok(client_id) =
                        writer.send_conn_res(&self.cluster_actor_sender, request).await
                    else {
                        error!("Failed to authenticate client stream");
                        continue;
                    };

                    let observer =
                        self.cluster_actor_sender.route_subscribe_topology_change().await?;

                    let stream_writer = writer.run(observer);

                    let client_controller = ClientController {
                        cluster_actor_sender: self.cluster_actor_sender.clone(),
                        cache_manager: self.cache_manager.clone(),
                    };
                    let listener = ClientStreamReader { client_id, r: read_half };
                    tokio::spawn(listener.handle_client_stream(client_controller, stream_writer));
                },
            }
        }
        Ok(())
    }
}

fn init_logs() -> SdkLoggerProvider {
    static RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
        Resource::builder_empty()
            .with_service_name("duva")
            .with_attribute(KeyValue::new("instance_id", uuid::Uuid::now_v7().to_string()))
            .build()
    });

    use opentelemetry_otlp::LogExporter;
    let exporter = LogExporter::builder()
        .with_http()
        .with_timeout(Duration::from_secs(2))
        .with_endpoint("http://localhost:4318/v1/logs")
        .build()
        .expect("Failed to create log exporter");

    SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(RESOURCE.clone())
        .build()
}
