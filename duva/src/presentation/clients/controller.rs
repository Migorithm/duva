use super::request::ClientRequest;
use crate::actor_registry::ActorRegistry;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::commands::{ClusterCommand, ConsensusClientResponse};
use crate::domains::config_actors::command::ConfigResponse;
use crate::domains::config_actors::config_manager::ConfigManager;
use crate::domains::query_parsers::QueryIO;
use crate::domains::saves::actor::SaveTarget;
use crate::presentation::clients::request::ClientAction;
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;

use std::sync::atomic::Ordering;

#[derive(Clone)]
pub(crate) struct ClientController {
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}

impl ClientController {
    pub(crate) fn new(actor_registry: ActorRegistry) -> Self {
        Self {
            cluster_communication_manager: actor_registry.cluster_communication_manager.clone(),
            cache_manager: actor_registry.cache_manager,
            config_manager: actor_registry.config_manager,
        }
    }

    pub(crate) async fn handle(
        &self,
        cmd: ClientAction,
        current_index: Option<u64>,
    ) -> anyhow::Result<QueryIO> {
        let response = match cmd {
            ClientAction::Ping => QueryIO::SimpleString("PONG".into()),
            ClientAction::Echo(val) => QueryIO::BulkString(val),
            ClientAction::Set { key, value } => QueryIO::SimpleString(
                self.cache_manager.route_set(key, value, None, current_index.unwrap()).await?,
            ),
            ClientAction::SetWithExpiry { key, value, expiry } => QueryIO::SimpleString(
                self.cache_manager
                    .route_set(key, value, Some(expiry), current_index.unwrap())
                    .await?,
            ),
            ClientAction::Append { key, value } => QueryIO::SimpleString(
                self.cache_manager
                    .route_append(key, value, current_index.unwrap())
                    .await?
                    .to_string(),
            ),
            ClientAction::Save => {
                let file_path = self.config_manager.get_filepath().await?;
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&file_path)
                    .await?;

                let repl_info = self.cluster_communication_manager.replication_info().await?;
                self.cache_manager
                    .route_save(
                        SaveTarget::File(file),
                        repl_info.replid,
                        repl_info.hwm.load(Ordering::Acquire),
                    )
                    .await?;

                QueryIO::Null
            },
            ClientAction::Get { key } => self.cache_manager.route_get(key).await?.into(),
            ClientAction::IndexGet { key, index } => {
                self.cache_manager.route_index_get(key, index).await?.into()
            },
            ClientAction::Keys { pattern } => self.cache_manager.route_keys(pattern).await?,
            ClientAction::Config { key, value } => {
                let res = self.config_manager.route_get((key, value)).await?;

                match res {
                    ConfigResponse::Dir(value) => QueryIO::SimpleString(format!("dir {}", value)),
                    ConfigResponse::DbFileName(value) => QueryIO::SimpleString(value),
                    _ => QueryIO::Err("Invalid operation".into()),
                }
            },
            ClientAction::Delete { keys } => {
                QueryIO::SimpleString(self.cache_manager.route_delete(keys).await?.to_string())
            },
            ClientAction::Exists { keys } => {
                QueryIO::SimpleString(self.cache_manager.route_exists(keys).await?.to_string())
            },
            ClientAction::Info => QueryIO::BulkString(
                self.cluster_communication_manager
                    .replication_info()
                    .await?
                    .vectorize()
                    .join("\r\n"),
            ),
            ClientAction::ClusterInfo => {
                self.cluster_communication_manager.cluster_info().await?.into()
            },
            ClientAction::ClusterNodes => self
                .cluster_communication_manager
                .cluster_nodes()
                .await?
                .into_iter()
                .map(|peer| peer.to_string())
                .collect::<Vec<_>>()
                .into(),
            ClientAction::ClusterForget(peer_identifier) => {
                match self.cluster_communication_manager.forget_peer(peer_identifier).await {
                    Ok(true) => QueryIO::SimpleString("OK".into()),
                    Ok(false) => QueryIO::Err("No such peer".into()),
                    Err(e) => QueryIO::Err(e.to_string()),
                }
            },
            ClientAction::ReplicaOf(peer_identifier) => {
                self.cluster_communication_manager.replicaof(peer_identifier.clone()).await;

                self.cluster_communication_manager.discover_cluster(peer_identifier).await?;

                QueryIO::SimpleString("OK".into())
            },
            ClientAction::Role => {
                let role = self.cluster_communication_manager.role();
                QueryIO::SimpleString(role.await?.to_string())
            },
            ClientAction::Ttl { key } => {
                QueryIO::SimpleString(self.cache_manager.route_ttl(key).await?)
            },
            ClientAction::Incr { key } => QueryIO::SimpleString(
                self.cache_manager.route_numeric_delta(key, 1, current_index.unwrap()).await?,
            ),
            ClientAction::Decr { key } => QueryIO::SimpleString(
                self.cache_manager.route_numeric_delta(key, -1, current_index.unwrap()).await?,
            ),
        };

        Ok(response)
    }

    pub(crate) async fn maybe_consensus_then_execute(
        &self,
        mut request: ClientRequest,
    ) -> anyhow::Result<QueryIO> {
        let consensus_res = self.maybe_consensus(&mut request).await?;

        match consensus_res {
            ConsensusClientResponse::AlreadyProcessed { key, index } => {
                // * Conversion! request has already been processed so we need to convert it to get
                request.action = ClientAction::IndexGet { key, index };
                self.handle(request.action, Some(index)).await
            },
            ConsensusClientResponse::LogIndex(optional_idx) => {
                let (res, _) = tokio::try_join!(
                    self.handle(request.action, optional_idx),
                    self.maybe_send_commit(optional_idx)
                )?;
                Ok(res)
            },
        }
    }

    async fn maybe_consensus(
        &self,
        request: &mut ClientRequest,
    ) -> anyhow::Result<ConsensusClientResponse> {
        // If the request doesn't require consensus, return Ok
        let Some(log) = request.action.to_write_request() else {
            return Ok(ConsensusClientResponse::LogIndex(None));
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cluster_communication_manager
            .send(ClusterCommand::LeaderReqConsensus {
                request: log,
                callback: tx,
                session_req: request.session_req.take(),
            })
            .await?;

        rx.await?
    }

    async fn maybe_send_commit(&self, log_index_num: Option<u64>) -> anyhow::Result<()> {
        if let Some(log_idx) = log_index_num {
            self.cluster_communication_manager
                .send(ClusterCommand::SendCommitHeartBeat { log_idx })
                .await?;
        }
        Ok(())
    }
}
