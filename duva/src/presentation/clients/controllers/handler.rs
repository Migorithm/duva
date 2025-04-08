use std::sync::atomic::Ordering;

use crate::{
    domains::cluster_actors::commands::ConsensusClientResponse,
    presentation::clients::request::ClientRequest,
    presentation::clusters::connection_manager::ClusterConnectionManager,
};

use super::*;

impl ClientController<Handler> {
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
                QueryIO::SimpleString(format!("OK RINDEX {}", current_index.unwrap()))
            },
            ClientAction::SetWithExpiry { key, value, expiry } => {
                let cache_entry =
                    CacheEntry::KeyValueExpiry(key.to_owned(), value.to_string(), expiry);
                self.cache_manager.route_set(cache_entry).await?;
                QueryIO::SimpleString(format!("OK RINDEX {}", current_index.unwrap()))
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
            ClientAction::Get { key } => self.cache_manager.route_get(key).await?,
            ClientAction::IndexGet { key, index } => {
                self.cache_manager.route_index_get(key, index).await?
            },
            ClientAction::Keys { pattern } => self.cache_manager.route_keys(pattern).await?,
            ClientAction::Config { key, value } => {
                let res = self.config_manager.route_get((key, value)).await?;

                match res {
                    ConfigResponse::Dir(value) => QueryIO::Array(vec![
                        QueryIO::BulkString("dir".into()),
                        QueryIO::BulkString(value),
                    ]),
                    ConfigResponse::DbFileName(value) => QueryIO::BulkString(value),
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
            ClientAction::ClusterNodes => {
                self.cluster_communication_manager.cluster_nodes().await?.into()
            },
            ClientAction::ClusterForget(peer_identifier) => {
                match self.cluster_communication_manager.forget_peer(peer_identifier).await {
                    Ok(true) => QueryIO::SimpleString("OK".into()),
                    Ok(false) => QueryIO::Err("No such peer".into()),
                    Err(e) => QueryIO::Err(e.to_string()),
                }
            },
            ClientAction::ReplicaOf(peer_identifier) => {
                // TODO should check if the peer is in the cluster?
                self.cluster_communication_manager.replicaof(peer_identifier.clone()).await;

                let (tx, rx) = tokio::sync::oneshot::channel();
                ClusterConnectionManager(self.cluster_communication_manager.clone())
                    .discover_cluster(self.config_manager.port, peer_identifier, tx)
                    .await?;
                let _ = rx.await;
                QueryIO::SimpleString("OK".into())
            },
        };
        Ok(response)
    }

    // Manage the client requests & consensus
    pub(super) async fn maybe_consensus_then_execute(
        &self,
        mut requests: Vec<ClientRequest>,
    ) -> anyhow::Result<Vec<QueryIO>> {
        let consensus = try_join_all(requests.iter_mut().map(|r| self.maybe_consensus(r))).await?;

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

    pub(super) async fn maybe_consensus(
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
