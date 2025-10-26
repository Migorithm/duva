use crate::config::ENV;
use crate::domains::QueryIO;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::caches::cache_objects::{CacheEntry, CacheValue, TypedValue};
use crate::domains::cluster_actors::queue::ClusterActorSender;
use crate::domains::cluster_actors::{
    ClientMessage, ConsensusClientResponse, ConsensusRequest, SessionRequest,
};
use crate::domains::replications::LogEntry;
use crate::domains::saves::actor::SaveTarget;
use crate::prelude::PeerIdentifier;
use crate::presentation::clients::request::NonMutatingAction;

use crate::types::{BinBytes, Callback};
use tracing::info;

#[derive(Clone, Debug)]
pub(crate) struct ClientController {
    pub(crate) cache_manager: CacheManager,
    pub(crate) cluster_actor_sender: ClusterActorSender,
}

impl ClientController {
    pub(crate) async fn handle_non_mutating(
        &self,
        non_mutating: NonMutatingAction,
    ) -> anyhow::Result<QueryIO> {
        use NonMutatingAction::*;

        let response = match non_mutating {
            Ping => QueryIO::SimpleString(BinBytes::new("PONG")),
            Echo(val) => QueryIO::BulkString(BinBytes::new(val)),

            Save => {
                let file_path = ENV.get_filepath();
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&file_path)
                    .await?;

                let repl_state = self.cluster_actor_sender.route_get_node_state().await?;
                self.cache_manager
                    .route_save(
                        SaveTarget::File(file),
                        repl_state.replid,
                        repl_state.last_log_index,
                    )
                    .await?;

                QueryIO::Null
            },
            Get { key } => self.cache_manager.route_get(key).await?.into(),
            MGet { keys } => {
                let res = self.cache_manager.route_mget(keys).await;
                QueryIO::Array(
                    res.into_iter()
                        .map(|entry| match entry {
                            Some(CacheEntry {
                                value: CacheValue { value: TypedValue::String(s), .. },
                                ..
                            }) => QueryIO::BulkString(s.into()),
                            _ => QueryIO::Null,
                        })
                        .collect(),
                )
            },
            IndexGet { key, index } => self.cache_manager.route_index_get(key, index).await?.into(),
            Keys { pattern } => {
                let res = self.cache_manager.route_keys(pattern).await;
                QueryIO::Array(
                    res.into_iter().map(|s| QueryIO::BulkString(BinBytes::new(s))).collect(),
                )
            },
            Config { key, value } => {
                match (key.to_lowercase().as_str(), value.to_lowercase().as_str()) {
                    ("get", "dir") => format!("dir {}", ENV.dir).into(),
                    ("get", "dbfilename") => ENV.dbfilename.clone().into(),
                    _ => Err(anyhow::anyhow!("Invalid command"))?,
                }
            },

            Exists { keys } => QueryIO::SimpleString(BinBytes::new(
                self.cache_manager.route_exists(keys).await?.to_string(),
            )),
            Info => QueryIO::BulkString(BinBytes::new(
                self.cluster_actor_sender.route_get_node_state().await?.vectorize().join("\r\n"),
            )),
            ClusterInfo => self.cluster_actor_sender.route_get_cluster_info().await?.into(),
            ClusterNodes => self
                .cluster_actor_sender
                .route_cluster_nodes()
                .await?
                .into_iter()
                .map(|peer| peer.format(&PeerIdentifier::new(&ENV.host, ENV.port)))
                .collect::<Vec<_>>()
                .into(),
            ClusterForget(peer_identifier) => {
                match self.cluster_actor_sender.route_forget_peer(peer_identifier).await {
                    Ok(true) => QueryIO::SimpleString(BinBytes::new("OK")),
                    Ok(false) => QueryIO::Err(BinBytes::new("No such peer")),
                    Err(e) => QueryIO::Err(BinBytes::new(e.to_string())),
                }
            },
            ClusterMeet(peer_identifier, option) => {
                self.cluster_actor_sender.route_cluster_meet(peer_identifier, option).await?.into()
            },
            ClusterReshard => self.cluster_actor_sender.route_cluster_reshard().await?.into(),
            ReplicaOf(peer_identifier) => {
                self.cluster_actor_sender.route_replicaof(peer_identifier.clone()).await?;
                QueryIO::SimpleString(BinBytes::new("OK"))
            },
            Role => self.cluster_actor_sender.route_get_roles().await?.into(),
            Ttl { key } => {
                QueryIO::SimpleString(BinBytes::new(self.cache_manager.route_ttl(key).await?))
            },

            LLen { key } => {
                let len = self.cache_manager.route_llen(key).await?;
                QueryIO::SimpleString(BinBytes::new(len.to_string()))
            },
            LRange { key, start, end } => {
                let values = self.cache_manager.route_lrange(key, start, end).await?;
                QueryIO::Array(
                    values.into_iter().map(|v| QueryIO::BulkString(BinBytes::new(v))).collect(),
                )
            },
            LIndex { key, index } => self.cache_manager.route_lindex(key, index).await?.into(),
        };
        info!("{response:?}");
        Ok(response)
    }

    pub(crate) async fn handle_mutating(
        &self,
        session_req: SessionRequest,
        entry: LogEntry,
    ) -> anyhow::Result<QueryIO> {
        // * Consensus / Persisting logs
        let (callback, res) = Callback::create();
        self.cluster_actor_sender
            .send(ClientMessage::LeaderReqConsensus(ConsensusRequest {
                entry,
                callback,
                session_req: Some(session_req),
            }))
            .await?;

        let result = match res.recv().await {
            ConsensusClientResponse::Result(result) => result,
            ConsensusClientResponse::AlreadyProcessed { key: keys, request_id } => {
                // * Conversion! request has already been processed so we need to convert it to get
                let action = NonMutatingAction::MGet { keys };
                self.handle_non_mutating(action).await
            },
            ConsensusClientResponse::Err { reason, request_id } => Err(anyhow::anyhow!(reason)),
        }?;

        Ok(result)
    }
}
