use crate::adapters::io::tokio_stream::TokioStreamListenerFactory;
use crate::services::config::manager::ConfigManager;
use crate::services::config::ConfigResponse;

use crate::services::interface::TWriterFactory;
use crate::services::interface::{TCancellationTokenFactory, TCancellationWatcher};
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use crate::services::statefuls::persist::actor::PersistActor;
use crate::services::statefuls::persist::endec::encoder::encoding_processor::SavingProcessor;

use arguments::ClientRequestArguments;
use request::ClientRequest;
use stream_manager::{ClientStream, ClientStreamManager};
use tokio::select;

pub mod arguments;
pub mod request;
pub mod stream_manager;

pub(crate) struct ClientRequestController {
    config_manager: ConfigManager,
    cache_manager: &'static CacheManager,
    ttl_manager: TtlSchedulerInbox,
}

impl ClientRequestController {
    pub(crate) fn new(
        config_manager: ConfigManager,
        cache_manager: &'static CacheManager,
        ttl_manager: TtlSchedulerInbox,
    ) -> Self {
        ClientRequestController {
            config_manager,
            cache_manager,
            ttl_manager,
        }
    }

    pub(crate) async fn handle<T: TWriterFactory>(
        &self,
        mut cancellation_token: impl TCancellationWatcher,
        cmd: ClientRequest,
        args: ClientRequestArguments,
    ) -> anyhow::Result<QueryIO> {
        if cancellation_token.watch() {
            let err = QueryIO::Err("Error operation cancelled due to timeout".to_string());
            return Ok(err);
        }

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            ClientRequest::Ping => QueryIO::SimpleString("PONG".to_string()),
            ClientRequest::Echo => args.first().ok_or(anyhow::anyhow!("Not exists"))?.clone(),
            ClientRequest::Set => {
                let cache_entry = args.take_set_args()?;
                self.cache_manager
                    .route_set(cache_entry, self.ttl_manager.clone())
                    .await?
            }
            ClientRequest::Save => {
                // spawn save actor
                let outbox = PersistActor::<SavingProcessor<T>>::run(
                    self.config_manager.get_filepath().await?,
                    self.cache_manager.inboxes.len(),
                )
                .await?;

                self.cache_manager.route_save(outbox).await;

                QueryIO::Null
            }
            ClientRequest::Get => {
                let key = args.take_get_args()?;
                self.cache_manager.route_get(key).await?
            }
            ClientRequest::Keys => {
                let pattern = args.take_keys_pattern()?;
                self.cache_manager.route_keys(pattern).await?
            }
            // modify we have to add a new command
            ClientRequest::Config => {
                let cmd = args.take_config_args()?;
                let res = self.config_manager.route_get(cmd).await?;

                match res {
                    ConfigResponse::Dir(value) => QueryIO::Array(vec![
                        QueryIO::BulkString("dir".into()),
                        QueryIO::BulkString(value),
                    ]),
                    ConfigResponse::DbFileName(value) => QueryIO::BulkString(value),
                    _ => QueryIO::Err("Invalid operation".into()),
                }
            }
            ClientRequest::Delete => panic!("Not implemented"),

            ClientRequest::Info => QueryIO::BulkString(
                self.config_manager
                    .replication_info()
                    .await?
                    .vectorize()
                    .join("\r\n"),
            ),
        };
        Ok(response)
    }

    pub async fn run_client_connection_handling_actor(
        &'static self,
        bind_addr: String,
        cancellation_factory: impl TCancellationTokenFactory,
        stop_sentinel_recv: tokio::sync::oneshot::Receiver<()>,
    ) {
        let client_stream_listener = TokioStreamListenerFactory.create_listner(&bind_addr).await;
        let mut conn_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(100);
        select! {
            _ = stop_sentinel_recv => {
                // Reconnection logic should be implemented by client?
                conn_handlers.iter().for_each(|handle| handle.abort());
            },
            _ = async {
                    while let Ok((stream, _)) = client_stream_listener.accept().await {
                        // TODO refactoring
                        let query_manager = ClientStreamManager::new(
                            ClientStream(stream),
                            self,
                        );

                        conn_handlers.push(tokio::spawn(
                            query_manager.handle_single_client_stream::<tokio::fs::File>(
                                cancellation_factory.clone()
                            ),
                        ));
                    }
                } =>{}
        };
    }
}