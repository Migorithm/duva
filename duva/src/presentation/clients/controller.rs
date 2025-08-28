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
use crate::presentation::clients::request::{ClientAction, NonMutatingAction};
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;
use crate::types::Callback;
use chrono::DateTime;
use std::sync::atomic::Ordering;
use tracing::info;

#[derive(Clone, Debug)]
pub(crate) struct ClientController {
    pub(crate) cache_manager: CacheManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}

impl ClientController {
    pub(crate) async fn handle(
        &self,
        cmd: ClientAction,
        current_index: Option<u64>,
    ) -> anyhow::Result<QueryIO> {
        let response = match cmd {
            | ClientAction::Mutating(write_req) => match write_req {
                | LogEntry::Set { key, value, expires_at } => {
                    let mut entry = CacheEntry::new(key, value.as_str());
                    if let Some(expires_at) = expires_at {
                        entry =
                            entry.with_expiry(DateTime::from_timestamp_millis(expires_at).unwrap())
                    }
                    QueryIO::SimpleString(
                        self.cache_manager.route_set(entry, current_index.unwrap()).await?.into(),
                    )
                },

                | LogEntry::Append { key, value } => QueryIO::SimpleString(
                    self.cache_manager.route_append(key, value).await?.to_string().into(),
                ),
                | LogEntry::Delete { keys } => QueryIO::SimpleString(
                    self.cache_manager.route_delete(keys).await?.to_string().into(),
                ),

                | LogEntry::IncrBy { key, delta: value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_numeric_delta(key, value, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::DecrBy { key, delta: value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_numeric_delta(key, -value, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::LPush { key, value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_lpush(key, value, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::LPushX { key, value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_lpushx(key, value, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::LPop { key, count } => {
                    let values = self.cache_manager.route_lpop(key, count).await?;
                    if values.is_empty() {
                        return Ok(QueryIO::Null);
                    }
                    QueryIO::Array(
                        values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect(),
                    )
                },
                | LogEntry::RPush { key, value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_rpush(key, value, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::RPushX { key, value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_rpushx(key, value, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::RPop { key, count } => {
                    let values = self.cache_manager.route_rpop(key, count).await?;
                    if values.is_empty() {
                        return Ok(QueryIO::Null);
                    }
                    QueryIO::Array(
                        values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect(),
                    )
                },
                | LogEntry::LTrim { key, start, end } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_ltrim(key, start, end, current_index.unwrap())
                        .await?
                        .into(),
                ),
                | LogEntry::LSet { key, index, value } => QueryIO::SimpleString(
                    self.cache_manager
                        .route_lset(key, index, value, current_index.unwrap())
                        .await?
                        .into(),
                ),

                | _ => unreachable!(),
            },

            | ClientAction::NonMutating(non_mutating) => match non_mutating {
                | NonMutatingAction::Ping => QueryIO::SimpleString("PONG".into()),
                | NonMutatingAction::Echo(val) => QueryIO::BulkString(val.into()),

                | NonMutatingAction::Save => {
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
                | NonMutatingAction::Get { key } => self.cache_manager.route_get(key).await?.into(),
                | NonMutatingAction::MGet { keys } => {
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
                | NonMutatingAction::IndexGet { key, index } => {
                    self.cache_manager.route_index_get(key, index).await?.into()
                },
                | NonMutatingAction::Keys { pattern } => {
                    let res = self.cache_manager.route_keys(pattern).await;
                    QueryIO::Array(res.into_iter().map(|s| QueryIO::BulkString(s.into())).collect())
                },
                | NonMutatingAction::Config { key, value } => {
                    match (key.to_lowercase().as_str(), value.to_lowercase().as_str()) {
                        | ("get", "dir") => format!("dir {}", ENV.dir).into(),
                        | ("get", "dbfilename") => ENV.dbfilename.clone().into(),
                        | _ => Err(anyhow::anyhow!("Invalid command"))?,
                    }
                },

                | NonMutatingAction::Exists { keys } => QueryIO::SimpleString(
                    self.cache_manager.route_exists(keys).await?.to_string().into(),
                ),
                | NonMutatingAction::Info => QueryIO::BulkString(
                    self.cluster_communication_manager
                        .route_get_replication_state()
                        .await?
                        .vectorize()
                        .join("\r\n")
                        .into(),
                ),
                | NonMutatingAction::ClusterInfo => {
                    self.cluster_communication_manager.route_get_cluster_info().await?.into()
                },
                | NonMutatingAction::ClusterNodes => self
                    .cluster_communication_manager
                    .route_cluster_nodes()
                    .await?
                    .into_iter()
                    .map(|peer| peer.format(&PeerIdentifier::new(&ENV.host, ENV.port)))
                    .collect::<Vec<_>>()
                    .into(),
                | NonMutatingAction::ClusterForget(peer_identifier) => {
                    match self
                        .cluster_communication_manager
                        .route_forget_peer(peer_identifier)
                        .await
                    {
                        | Ok(true) => QueryIO::SimpleString("OK".into()),
                        | Ok(false) => QueryIO::Err("No such peer".into()),
                        | Err(e) => QueryIO::Err(e.to_string().into()),
                    }
                },
                | NonMutatingAction::ClusterMeet(peer_identifier, option) => self
                    .cluster_communication_manager
                    .route_cluster_meet(peer_identifier, option)
                    .await?
                    .into(),
                | NonMutatingAction::ClusterReshard => {
                    self.cluster_communication_manager.route_cluster_reshard().await?.into()
                },
                | NonMutatingAction::ReplicaOf(peer_identifier) => {
                    self.cluster_communication_manager
                        .route_replicaof(peer_identifier.clone())
                        .await?;
                    QueryIO::SimpleString("OK".into())
                },
                | NonMutatingAction::Role => {
                    self.cluster_communication_manager.route_get_roles().await?.into()
                },
                | NonMutatingAction::Ttl { key } => {
                    QueryIO::SimpleString(self.cache_manager.route_ttl(key).await?.into())
                },

                | NonMutatingAction::LLen { key } => {
                    let len = self.cache_manager.route_llen(key).await?;
                    QueryIO::SimpleString(len.to_string().into())
                },
                | NonMutatingAction::LRange { key, start, end } => {
                    let values = self.cache_manager.route_lrange(key, start, end).await?;
                    QueryIO::Array(
                        values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect(),
                    )
                },
                | NonMutatingAction::LIndex { key, index } => {
                    self.cache_manager.route_lindex(key, index).await?.into()
                },
            },
        };
        info!("{response:?}");
        Ok(response)
    }

    pub(crate) async fn make_consensus(
        &self,
        session_req: SessionRequest,
        cli_action: &mut ClientAction,
    ) -> anyhow::Result<u64> {
        let write_req = cli_action.to_write_request();

        let (tx, consensus_res) = Callback::create();
        self.cluster_communication_manager
            .send(ClientMessage::LeaderReqConsensus(ConsensusRequest::new(
                write_req,
                tx,
                Some(session_req),
            )))
            .await?;

        match consensus_res.recv().await {
            | ConsensusClientResponse::AlreadyProcessed { key: keys, index } => {
                // * Conversion! request has already been processed so we need to convert it to get
                let action = NonMutatingAction::MGet { keys }.into();
                *cli_action = action;
                Ok(index)
            },
            | ConsensusClientResponse::LogIndex(idx) => Ok(idx),
            | ConsensusClientResponse::Err(error_msg) => Err(anyhow::anyhow!(error_msg)),
        }
    }
}
