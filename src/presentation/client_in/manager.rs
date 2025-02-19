use super::request::ClientRequest;
use super::stream::ClientStream;
use crate::actor_registry::ActorRegistry;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::config_actors::command::ConfigResponse;
use crate::domains::storage::cache_objects::CacheEntry;
use crate::presentation::cluster_in::communication_manager::ClusterCommunicationManager;
use crate::services::config_manager::ConfigManager;

use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerManager;

use crate::services::statefuls::snapshot::save::actor::SaveTarget;
use tokio::net::{TcpListener, TcpStream};
use tokio::select;

#[derive(Clone)]
pub(crate) struct ClientManager {
    pub(crate) ttl_manager: TtlSchedulerManager,
    pub(crate) cache_manager: CacheManager,
    pub(crate) config_manager: ConfigManager,
    pub(crate) cluster_communication_manager: ClusterCommunicationManager,
}

impl ClientManager {
    pub(crate) fn new(actor_registry: ActorRegistry) -> Self {
        ClientManager {
            cluster_communication_manager: actor_registry.cluster_communication_manager(),
            ttl_manager: actor_registry.ttl_manager,
            cache_manager: actor_registry.cache_manager,
            config_manager: actor_registry.config_manager,
        }
    }

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

    /// Run while loop accepting stream and if the sentinel is received, abort the tasks
    pub async fn accept_client_connections(
        self,
        stop_sentinel_recv: tokio::sync::oneshot::Receiver<()>,
        client_stream_listener: TcpListener,
    ) {
        let mut conn_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(100);

        select! {
            // The following closure doesn't take the ownership of the `conn_handlers` which enables us to abort the tasks
            // when the sentinel is received.
            _ = async {
                    while let Ok((stream, _)) = client_stream_listener.accept().await {
                        conn_handlers.push(tokio::spawn(
                            self.clone().handle_client_stream(stream),
                        ));
                    }
                } =>{

                }
            _ = stop_sentinel_recv => {
                // Reconnection logic should be implemented by client?
                    conn_handlers.iter().for_each(|handle| handle.abort());
                },

        };
    }

    async fn handle_client_stream(self, stream: TcpStream) {
        let mut stream = ClientStream(stream);
        loop {
            let Ok(requests) = stream.extract_query().await else {
                eprintln!("invalid user request");
                continue;
            };

            for request in requests.into_iter() {
                // ! if request requires consensus, send it to cluster manager so tranasction inputs can be logged and consensus can be made
                let Ok(optional_log_offset) = self.try_consensus(&request).await else {
                    let _ = stream.write(QueryIO::Err("Consensus failed".into())).await;
                    continue;
                };

                // apply state change
                let res = match self.handle(request).await {
                    Ok(response) => response,
                    Err(e) => QueryIO::Err(e.to_string().into()),
                };

                if let Err(e) = stream.write(res).await {
                    if e.should_break() {
                        break;
                    }
                }
            }
        }
    }

    async fn try_consensus(&self, request: &ClientRequest) -> anyhow::Result<Option<u64>> {
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
}
