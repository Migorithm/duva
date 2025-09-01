use crate::config::ENV;
use crate::domains::QueryIO;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::caches::cache_objects::{CacheEntry, CacheValue, TypedValue};
use crate::domains::cluster_actors::{
    ClientMessage, ConsensusClientResponse, ConsensusRequest, SessionRequest,
};
use crate::domains::operation_logs::LogEntry;
use crate::domains::saves::actor::SaveTarget;
use crate::prelude::PeerIdentifier;
use crate::presentation::clients::request::NonMutatingAction;
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;
use crate::types::Callback;
use std::sync::atomic::Ordering;
use tracing::info;

#[derive(Clone, Debug)]
pub(crate) struct ClientController {
    pub(crate) cache_manager: CacheManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}

impl ClientController {
    pub(crate) async fn handle_non_mutating(
        &self,
        non_mutating: NonMutatingAction,
    ) -> anyhow::Result<QueryIO> {
        use NonMutatingAction::*;

        let response = match non_mutating {
            | Ping => QueryIO::SimpleString("PONG".into()),
            | Echo(val) => QueryIO::BulkString(val.into()),

            | Save => {
                let file_path = ENV.get_filepath();
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&file_path)
                    .await?;

                let repl_info =
                    self.cluster_communication_manager.route_get_replication_state().await?;
                self.cache_manager
                    .route_save(
                        SaveTarget::File(file),
                        repl_info.replid,
                        repl_info.con_idx.load(Ordering::Acquire),
                    )
                    .await?;

                QueryIO::Null
            },
            | Get { key } => self.cache_manager.route_get(key).await?.into(),
            | MGet { keys } => {
                let res = self.cache_manager.route_mget(keys).await;
                QueryIO::Array(
                    res.into_iter()
                        .map(|entry| match entry {
                            | Some(CacheEntry {
                                value: CacheValue { value: TypedValue::String(s), .. },
                                ..
                            }) => QueryIO::BulkString(s),
                            | _ => QueryIO::Null,
                        })
                        .collect(),
                )
            },
            | IndexGet { key, index } => {
                self.cache_manager.route_index_get(key, index).await?.into()
            },
            | Keys { pattern } => {
                let res = self.cache_manager.route_keys(pattern).await;
                QueryIO::Array(res.into_iter().map(|s| QueryIO::BulkString(s.into())).collect())
            },
            | Config { key, value } => {
                match (key.to_lowercase().as_str(), value.to_lowercase().as_str()) {
                    | ("get", "dir") => format!("dir {}", ENV.dir).into(),
                    | ("get", "dbfilename") => ENV.dbfilename.clone().into(),
                    | _ => Err(anyhow::anyhow!("Invalid command"))?,
                }
            },

            | Exists { keys } => QueryIO::SimpleString(
                self.cache_manager.route_exists(keys).await?.to_string().into(),
            ),
            | Info => QueryIO::BulkString(
                self.cluster_communication_manager
                    .route_get_replication_state()
                    .await?
                    .vectorize()
                    .join("\r\n")
                    .into(),
            ),
            | ClusterInfo => {
                self.cluster_communication_manager.route_get_cluster_info().await?.into()
            },
            | ClusterNodes => self
                .cluster_communication_manager
                .route_cluster_nodes()
                .await?
                .into_iter()
                .map(|peer| peer.format(&PeerIdentifier::new(&ENV.host, ENV.port)))
                .collect::<Vec<_>>()
                .into(),
            | ClusterForget(peer_identifier) => {
                match self.cluster_communication_manager.route_forget_peer(peer_identifier).await {
                    | Ok(true) => QueryIO::SimpleString("OK".into()),
                    | Ok(false) => QueryIO::Err("No such peer".into()),
                    | Err(e) => QueryIO::Err(e.to_string().into()),
                }
            },
            | ClusterMeet(peer_identifier, option) => self
                .cluster_communication_manager
                .route_cluster_meet(peer_identifier, option)
                .await?
                .into(),
            | ClusterReshard => {
                self.cluster_communication_manager.route_cluster_reshard().await?.into()
            },
            | ReplicaOf(peer_identifier) => {
                self.cluster_communication_manager.route_replicaof(peer_identifier.clone()).await?;
                QueryIO::SimpleString("OK".into())
            },
            | Role => self.cluster_communication_manager.route_get_roles().await?.into(),
            | Ttl { key } => QueryIO::SimpleString(self.cache_manager.route_ttl(key).await?.into()),

            | LLen { key } => {
                let len = self.cache_manager.route_llen(key).await?;
                QueryIO::SimpleString(len.to_string().into())
            },
            | LRange { key, start, end } => {
                let values = self.cache_manager.route_lrange(key, start, end).await?;
                QueryIO::Array(values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect())
            },
            | LIndex { key, index } => self.cache_manager.route_lindex(key, index).await?.into(),
        };
        info!("{response:?}");
        Ok(response)
    }

    pub(crate) async fn handle_mutating(
        &self,
        session_req: SessionRequest,
        write_req: LogEntry,
    ) -> anyhow::Result<QueryIO> {
        // * Consensus / Persisting logs
        let (tx, res) = Callback::create();
        self.cluster_communication_manager
            .send(ClientMessage::LeaderReqConsensus(ConsensusRequest::new(
                write_req.clone(), //TODO let cache actor decide the return. No need for copy over here.
                tx,
                Some(session_req),
            )))
            .await?;

        let current_index = match res.recv().await {
            | ConsensusClientResponse::AlreadyProcessed { key: keys, .. } => {
                // * Conversion! request has already been processed so we need to convert it to get
                //TODO revisit required. When it has been already processed, just route this to reader controller
                let _action = NonMutatingAction::MGet { keys };
                self.handle_non_mutating(_action).await
            },
            | ConsensusClientResponse::Result(result) => Ok(result),
            | ConsensusClientResponse::Err(error_msg) => Err(anyhow::anyhow!(error_msg)),
        }?;

        Ok(current_index)
    }
}
