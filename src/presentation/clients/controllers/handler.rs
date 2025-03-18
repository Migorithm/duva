use crate::domains::cluster_actors::commands::WriteConsensusResponse;

use super::*;

impl ClientController<Handler> {
    pub(crate) async fn handle(&self, cmd: ClientRequest) -> anyhow::Result<QueryIO> {
        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            ClientRequest::Ping => QueryIO::SimpleString("PONG".into()),
            ClientRequest::Echo(val) => QueryIO::BulkString(val.into()),
            ClientRequest::Set { key, value } => {
                let cache_entry = CacheEntry::KeyValue(key.to_owned(), value.to_string());

                self.cache_manager.route_set(cache_entry).await?
            },
            ClientRequest::SetWithExpiry { key, value, expiry } => {
                let cache_entry =
                    CacheEntry::KeyValueExpiry(key.to_owned(), value.to_string(), expiry);
                self.cache_manager.route_set(cache_entry).await?
            },
            ClientRequest::Save => {
                let file_path = self.config_manager.get_filepath().await?;
                let file =
                    tokio::fs::OpenOptions::new().write(true).create(true).open(&file_path).await?;

                let repl_info = self.cluster_communication_manager.replication_info().await?;
                self.cache_manager
                    .route_save(SaveTarget::File(file), repl_info.leader_replid, repl_info.hwm)
                    .await?;

                QueryIO::Null
            },
            ClientRequest::Get { key } => self.cache_manager.route_get(key).await?,
            ClientRequest::Keys { pattern } => self.cache_manager.route_keys(pattern).await?,
            ClientRequest::Config { key, value } => {
                let res = self.config_manager.route_get((key, value)).await?;

                match res {
                    ConfigResponse::Dir(value) => QueryIO::Array(vec![
                        QueryIO::BulkString("dir".into()),
                        QueryIO::BulkString(value.into()),
                    ]),
                    ConfigResponse::DbFileName(value) => QueryIO::BulkString(value.into()),
                    _ => QueryIO::Err("Invalid operation".into()),
                }
            },
            ClientRequest::Delete { key: _ } => panic!("Not implemented"),
            ClientRequest::Info => QueryIO::BulkString(
                self.cluster_communication_manager
                    .replication_info()
                    .await?
                    .vectorize()
                    .join("\r\n")
                    .into(),
            ),
            ClientRequest::ClusterInfo => {
                self.cluster_communication_manager.cluster_info().await?.into()
            },
            ClientRequest::ClusterNodes => {
                self.cluster_communication_manager.cluster_nodes().await?.into()
            },

            ClientRequest::ClusterForget(peer_identifier) => {
                match self.cluster_communication_manager.forget_peer(peer_identifier).await {
                    Ok(true) => QueryIO::SimpleString("OK".into()),
                    Ok(false) => QueryIO::Err("No such peer".into()),
                    Err(e) => QueryIO::Err(e.to_string().into()),
                }
            },
            ClientRequest::ReplicaOf(peer_identifier) => {
                match self.cluster_connection_manager.discover_cluster(self.config_manager.port, peer_identifier).await {
                    Ok(_) => QueryIO::SimpleString("OK".into()),
                    Err(e) => QueryIO::Err(e.to_string().into()),
                }
            }
        };
        Ok(response)
    }

    // Manage the client requests & consensus
    pub(super) async fn maybe_consensus_then_execute(
        &self,
        requests: Vec<ClientRequest>,
    ) -> anyhow::Result<Vec<QueryIO>> {
        let consensus = try_join_all(requests.iter().map(|r| self.maybe_consensus(&r))).await?;

        // apply write operation to the state machine if it's a write request
        let mut results = Vec::with_capacity(requests.len());
        for (request, log_index_num) in requests.into_iter().zip(consensus.into_iter()) {
            let (res, _) =
                tokio::try_join!(self.handle(request), self.maybe_send_commit(log_index_num))?;
            results.push(res);
        }
        Ok(results)
    }

    pub(super) async fn maybe_consensus(
        &self,
        request: &ClientRequest,
    ) -> anyhow::Result<Option<LogIndex>> {
        // If the request doesn't require consensus, return Ok
        let Some(log) = request.to_write_request() else {
            return Ok(None);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cluster_communication_manager
            .send(ClusterCommand::LeaderReqConsensus { log, sender: tx })
            .await?;

        match rx.await? {
            //TODO remove option
            WriteConsensusResponse::LogIndex(log_index) => Ok(log_index),
            WriteConsensusResponse::Err(err) => Err(anyhow::anyhow!(err)),
        }
    }

    async fn maybe_send_commit(&self, log_index_num: Option<LogIndex>) -> anyhow::Result<()> {
        if let Some(log_idx) = log_index_num {
            self.cluster_communication_manager
                .send(ClusterCommand::SendCommitHeartBeat { log_idx })
                .await?;
        }
        Ok(())
    }
}
