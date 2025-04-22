use super::request::ClientRequest;
use crate::actor_registry::ActorRegistry;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::caches::cache_objects::CacheEntry;
use crate::domains::cluster_actors::commands::{ClusterCommand, ConsensusClientResponse};
use crate::domains::config_actors::command::ConfigResponse;
use crate::domains::config_actors::config_manager::ConfigManager;
use crate::domains::query_parsers::QueryIO;
use crate::domains::saves::actor::SaveTarget;
use crate::presentation::clients::request::ClientAction;
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;

use anyhow::Context;
use futures::future::try_join_all;
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
            cluster_communication_manager: actor_registry.cluster_communication_manager(),
            cache_manager: actor_registry.cache_manager,
            config_manager: actor_registry.config_manager,
        }
    }

    pub(crate) async fn handle(
        &self,
        cmd: ClientAction,
        current_index: Option<u64>,
    ) -> anyhow::Result<QueryIO> {
        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            ClientAction::Ping => QueryIO::SimpleString("PONG".into()),
            ClientAction::Echo(val) => QueryIO::BulkString(val),
            ClientAction::Set { key, value } => {
                let cache_entry = CacheEntry::KeyValue(key.to_owned(), value.to_string());
                self.cache_manager.route_set(cache_entry).await?;
                QueryIO::SimpleString(format!("s:{}|idx:{}", value, current_index.unwrap()))
            },
            ClientAction::SetWithExpiry { key, value, expiry } => {
                let cache_entry =
                    CacheEntry::KeyValueExpiry(key.to_owned(), value.to_string(), expiry);
                self.cache_manager.route_set(cache_entry).await?;
                QueryIO::SimpleString(format!("s:{}|idx:{}", value, current_index.unwrap()))
            },
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
            _ => QueryIO::Err("Invalid command".into()),
        };
        Ok(response)
    }

    // Manage the client requests & consensus
    pub(crate) async fn maybe_consensus_then_execute(
        &self,
        mut requests: Vec<ClientRequest>,
    ) -> anyhow::Result<Vec<QueryIO>> {
        let consensus = try_join_all(requests.iter_mut().map(|r| async {
            self.resolve_key_dependent_action(r).await?;
            self.maybe_consensus(r).await
        }))
        .await?;

        // apply write operation to the state machine if it's a write request
        let mut results = Vec::with_capacity(requests.len());
        for (request, log_index_num) in requests.into_iter().zip(consensus.into_iter()) {
            let (res, _) = tokio::try_join!(
                self.handle(request.action, log_index_num),
                self.maybe_send_commit(log_index_num)
            )?;
            results.push(res);
        }
        Ok(results)
    }
    async fn resolve_key_dependent_action(
        &self,
        request: &mut ClientRequest,
    ) -> anyhow::Result<()> {
        match &request.action {
            ClientAction::Incr { key } | ClientAction::Decr { key } => {
                if let Some(v) = self.cache_manager.route_get(&key).await? {
                    // Parse current value to u64, add 1, and handle errors
                    let num =
                        v.parse::<i64>().context("ERR value is not an integer or out of range")?;
                    // Handle potential overflow
                    let incremented = num
                        .checked_add(request.action.delta())
                        .context("ERR value is not an integer or out of range")?;

                    request.action =
                        ClientAction::Set { key: key.clone(), value: incremented.to_string() };
                } else {
                    request.action = ClientAction::Set {
                        key: key.clone(),
                        value: request.action.delta().to_string(),
                    };
                }
            },

            // Add other cases here for future store operations
            _ => {},
        };

        Ok(())
    }

    pub(crate) async fn maybe_consensus(
        &self,
        request: &mut ClientRequest,
    ) -> anyhow::Result<Option<u64>> {
        // If the request doesn't require consensus, return Ok
        let Some(log) = request.action.to_write_request() else {
            return Ok(None);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cluster_communication_manager
            .send(ClusterCommand::LeaderReqConsensus {
                log,
                callback: tx,
                session_req: request.session_req.take(),
            })
            .await?;

        match rx.await? {
            //TODO remove option
            ConsensusClientResponse::LogIndex(log_index) => Ok(log_index),
            ConsensusClientResponse::Err(err) => Err(anyhow::anyhow!(err)),
        }
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
