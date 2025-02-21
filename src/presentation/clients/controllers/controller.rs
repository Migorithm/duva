use super::*;

impl ClientController<Handler> {
    pub(crate) async fn handle(&self, cmd: ClientRequest) -> anyhow::Result<QueryIO> {
        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            ClientRequest::Ping => QueryIO::SimpleString("PONG".into()),
            ClientRequest::Echo(val) => QueryIO::BulkString(val.into()),
            ClientRequest::Set { key, value } => {
                let cache_entry = CacheEntry::KeyValue(key.to_owned(), value.to_string());

                self.cache_manager.route_set(cache_entry, self.ttl_manager.clone()).await?
            }
            ClientRequest::SetWithExpiry { key, value, expiry } => {
                let cache_entry =
                    CacheEntry::KeyValueExpiry(key.to_owned(), value.to_string(), expiry);
                self.cache_manager.route_set(cache_entry, self.ttl_manager.clone()).await?
            }
            ClientRequest::Save => {
                let file_path = self.config_manager.get_filepath().await?;
                let file =
                    tokio::fs::OpenOptions::new().write(true).create(true).open(&file_path).await?;

                let repl_info = self.cluster_communication_manager.replication_info().await?;
                self.cache_manager
                    .route_save(
                        SaveTarget::File(file),
                        repl_info.leader_repl_id,
                        repl_info.leader_repl_offset,
                    )
                    .await?;

                QueryIO::Null
            }
            ClientRequest::Get { key } => self.cache_manager.route_get(key).await?,
            ClientRequest::Keys { pattern } => self.cache_manager.route_keys(pattern).await?,
            // modify we have to add a new command
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
            }
            ClientRequest::Delete { key: _ } => panic!("Not implemented"),

            ClientRequest::Info => QueryIO::BulkString(
                self.cluster_communication_manager
                    .replication_info()
                    .await?
                    .vectorize()
                    .join("\r\n")
                    .into(),
            ),
            ClientRequest::ClusterInfo => QueryIO::Array(
                self.cluster_communication_manager
                    .cluster_info()
                    .await?
                    .into_iter()
                    .map(|x| QueryIO::BulkString(x.into()))
                    .collect(),
            ),
            ClientRequest::ClusterForget(peer_identifier) => {
                match self.cluster_communication_manager.forget_peer(peer_identifier).await {
                    Ok(true) => QueryIO::SimpleString("OK".into()),
                    Ok(false) => QueryIO::Err("No such peer".into()),
                    Err(e) => QueryIO::Err(e.to_string().into()),
                }
            }
        };
        Ok(response)
    }

    pub(super) async fn try_consensus(
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
        Ok(rx.await?)
    }

    // Manage the client requests & consensus
    pub(super) async fn handle_client_requests(
        &self,
        requests: Vec<ClientRequest>,
    ) -> anyhow::Result<Vec<QueryIO>> {
        // consensus first!
        let consensus = try_join_all(requests.iter().map(|r| self.try_consensus(&r))).await?;

        let mut results = Vec::with_capacity(requests.len());
        for (request, log_index_num) in requests.into_iter().zip(consensus.into_iter()) {
            // ! if request requires consensus, send it to cluster manager so tranasction inputs can be logged and consensus can be made
            // apply state change
            let res = self.handle(request).await?;
            // ! run stream.write(res) and state change operation to replicas at the same time
            if let Some(offset) = log_index_num {
                self.cluster_communication_manager
                    .send(ClusterCommand::SendCommitHeartBeat { offset })
                    .await?;
            }
            results.push(res);
        }
        Ok(results)
    }
}
