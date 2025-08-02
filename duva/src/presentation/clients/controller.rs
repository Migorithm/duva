use crate::config::ENV;
use crate::domains::QueryIO;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::caches::cache_objects::{CacheEntry, CacheValue, TypedValue};
use crate::domains::cluster_actors::{
    ClientMessage, ConsensusClientResponse, ConsensusRequest, SessionRequest,
};
use crate::domains::operation_logs::WriteRequest;
use crate::domains::saves::actor::SaveTarget;
use crate::prelude::PeerIdentifier;
use crate::presentation::clients::request::ClientAction;
use crate::presentation::clusters::communication_manager::ClusterCommunicationManager;
use crate::types::Callback;
use chrono::DateTime;
use std::sync::atomic::Ordering;

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
            | ClientAction::Ping => QueryIO::SimpleString("PONG".into()),
            | ClientAction::Echo(val) => QueryIO::BulkString(val.into()),
            | ClientAction::Set { key, value } => QueryIO::SimpleString(
                self.cache_manager
                    .route_set(CacheEntry::new(key, value.as_str()), current_index.unwrap())
                    .await?
                    .into(),
            ),
            | ClientAction::SetWithExpiry { key, value, expires_at } => QueryIO::SimpleString(
                self.cache_manager
                    .route_set(
                        CacheEntry::new(key, value.as_str())
                            .with_expiry(DateTime::from_timestamp_millis(expires_at).unwrap()),
                        current_index.unwrap(),
                    )
                    .await?
                    .into(),
            ),
            | ClientAction::Append { key, value } => QueryIO::SimpleString(
                self.cache_manager.route_append(key, value).await?.to_string().into(),
            ),
            | ClientAction::Save => {
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
                        repl_info.hwm.load(Ordering::Acquire),
                    )
                    .await?;

                QueryIO::Null
            },
            | ClientAction::Get { key } => self.cache_manager.route_get(key).await?.into(),
            | ClientAction::MGet { keys } => {
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
            | ClientAction::IndexGet { key, index } => {
                self.cache_manager.route_index_get(key, index).await?.into()
            },
            | ClientAction::Keys { pattern } => {
                let res = self.cache_manager.route_keys(pattern).await;
                QueryIO::Array(res.into_iter().map(|s| QueryIO::BulkString(s.into())).collect())
            },
            | ClientAction::Config { key, value } => {
                match (key.to_lowercase().as_str(), value.to_lowercase().as_str()) {
                    | ("get", "dir") => format!("dir {}", ENV.dir).into(),
                    | ("get", "dbfilename") => ENV.dbfilename.clone().into(),
                    | _ => Err(anyhow::anyhow!("Invalid command"))?,
                }
            },
            | ClientAction::Delete { keys } => QueryIO::SimpleString(
                self.cache_manager.route_delete(keys).await?.to_string().into(),
            ),
            | ClientAction::Exists { keys } => QueryIO::SimpleString(
                self.cache_manager.route_exists(keys).await?.to_string().into(),
            ),
            | ClientAction::Info => QueryIO::BulkString(
                self.cluster_communication_manager
                    .route_get_replication_state()
                    .await?
                    .vectorize()
                    .join("\r\n")
                    .into(),
            ),
            | ClientAction::ClusterInfo => {
                self.cluster_communication_manager.route_get_cluster_info().await?.into()
            },
            | ClientAction::ClusterNodes => self
                .cluster_communication_manager
                .route_cluster_nodes()
                .await?
                .into_iter()
                .map(|peer| peer.format(&PeerIdentifier::new(&ENV.host, ENV.port)))
                .collect::<Vec<_>>()
                .into(),
            | ClientAction::ClusterForget(peer_identifier) => {
                match self.cluster_communication_manager.route_forget_peer(peer_identifier).await {
                    | Ok(true) => QueryIO::SimpleString("OK".into()),
                    | Ok(false) => QueryIO::Err("No such peer".into()),
                    | Err(e) => QueryIO::Err(e.to_string().into()),
                }
            },
            | ClientAction::ClusterMeet(peer_identifier, option) => self
                .cluster_communication_manager
                .route_cluster_meet(peer_identifier, option)
                .await?
                .into(),
            | ClientAction::ClusterReshard => {
                self.cluster_communication_manager.route_cluster_reshard().await?.into()
            },
            | ClientAction::ReplicaOf(peer_identifier) => {
                self.cluster_communication_manager.route_replicaof(peer_identifier.clone()).await?;
                QueryIO::SimpleString("OK".into())
            },
            | ClientAction::Role => {
                let role = self.cluster_communication_manager.route_get_role();
                QueryIO::SimpleString(role.await?.to_string().into())
            },
            | ClientAction::Ttl { key } => {
                QueryIO::SimpleString(self.cache_manager.route_ttl(key).await?.into())
            },
            | ClientAction::Incr { key } => QueryIO::SimpleString(
                self.cache_manager
                    .route_numeric_delta(key, 1, current_index.unwrap())
                    .await?
                    .into(),
            ),
            | ClientAction::Decr { key } => QueryIO::SimpleString(
                self.cache_manager
                    .route_numeric_delta(key, -1, current_index.unwrap())
                    .await?
                    .into(),
            ),
            | ClientAction::IncrBy { key, increment } => QueryIO::SimpleString(
                self.cache_manager
                    .route_numeric_delta(key, increment, current_index.unwrap())
                    .await?
                    .into(),
            ),
            | ClientAction::DecrBy { key, decrement } => QueryIO::SimpleString(
                self.cache_manager
                    .route_numeric_delta(key, -decrement, current_index.unwrap())
                    .await?
                    .into(),
            ),
            | ClientAction::LPush { key, value } => QueryIO::SimpleString(
                self.cache_manager.route_lpush(key, value, current_index.unwrap()).await?.into(),
            ),
            | ClientAction::LPushX { key, value } => QueryIO::SimpleString(
                self.cache_manager.route_lpushx(key, value, current_index.unwrap()).await?.into(),
            ),
            | ClientAction::LPop { key, count } => {
                let values = self.cache_manager.route_lpop(key, count).await?;
                if values.is_empty() {
                    return Ok(QueryIO::Null);
                }
                QueryIO::Array(values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect())
            },
            | ClientAction::RPush { key, value } => QueryIO::SimpleString(
                self.cache_manager.route_rpush(key, value, current_index.unwrap()).await?.into(),
            ),
            | ClientAction::RPushX { key, value } => QueryIO::SimpleString(
                self.cache_manager.route_rpushx(key, value, current_index.unwrap()).await?.into(),
            ),
            | ClientAction::RPop { key, count } => {
                let values = self.cache_manager.route_rpop(key, count).await?;
                if values.is_empty() {
                    return Ok(QueryIO::Null);
                }
                QueryIO::Array(values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect())
            },
            | ClientAction::LTrim { key, start, end } => QueryIO::SimpleString(
                self.cache_manager
                    .route_ltrim(key, start, end, current_index.unwrap())
                    .await?
                    .into(),
            ),
            | ClientAction::LLen { key } => {
                let len = self.cache_manager.route_llen(key).await?;
                QueryIO::SimpleString(len.to_string().into())
            },
            | ClientAction::LRange { key, start, end } => {
                let values = self.cache_manager.route_lrange(key, start, end).await?;
                QueryIO::Array(values.into_iter().map(|v| QueryIO::BulkString(v.into())).collect())
            },
            | ClientAction::LIndex { key, index } => {
                self.cache_manager.route_lindex(key, index).await?.into()
            },
            | ClientAction::LSet { key, index, value } => QueryIO::SimpleString(
                self.cache_manager
                    .route_lset(key, index, value, current_index.unwrap())
                    .await?
                    .into(),
            ),
        };

        Ok(response)
    }

    pub(crate) async fn make_consensus(
        &self,
        session_req: SessionRequest,
        write_req: WriteRequest,
        cli_action: &mut ClientAction,
    ) -> anyhow::Result<u64> {
        let (tx, consensus_res) = Callback::create();

        self.cluster_communication_manager
            .send(ClientMessage::LeaderReqConsensus(ConsensusRequest::new(
                write_req,
                tx,
                Some(session_req),
            )))
            .await?;

        match consensus_res.await? {
            | ConsensusClientResponse::AlreadyProcessed { key: keys, index } => {
                // * Conversion! request has already been processed so we need to convert it to get
                let action = ClientAction::MGet { keys };
                *cli_action = action;
                Ok(index)
            },
            | ConsensusClientResponse::LogIndex(idx) => Ok(idx),
            | ConsensusClientResponse::Err(error_msg) => Err(anyhow::anyhow!(error_msg)),
        }
    }
}
