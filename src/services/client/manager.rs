use crate::services::client::arguments::ClientRequestArguments;
use crate::services::client::request::ClientRequest;
use crate::services::client::stream::ClientStream;
use crate::services::config::manager::ConfigManager;
use crate::services::config::ConfigResponse;
use crate::services::interface::TCancellationNotifier;
use crate::services::interface::TCancellationTokenFactory;
use crate::services::interface::TCancellationWatcher;
use crate::services::interface::TStream;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::cache::ttl::manager::TtlSchedulerInbox;
use crate::services::statefuls::persist::actor::PersistActor;
use crate::services::statefuls::persist::endec::encoder::encoding_processor::SavingProcessor;
use tokio::net::TcpListener;
use tokio::select;

pub(crate) struct ClientManager {
    config_manager: ConfigManager,
    cache_manager: &'static CacheManager,
    ttl_manager: TtlSchedulerInbox,
}

impl ClientManager {
    pub(crate) fn new(
        config_manager: ConfigManager,
        cache_manager: &'static CacheManager,
        ttl_manager: TtlSchedulerInbox,
    ) -> Self {
        ClientManager { config_manager, cache_manager, ttl_manager }
    }

    pub(crate) async fn handle(
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
                self.cache_manager.route_set(cache_entry, self.ttl_manager.clone()).await?
            }
            ClientRequest::Save => {
                // spawn save actor
                let outbox = PersistActor::<SavingProcessor>::run(
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
                self.config_manager.replication_info().await?.vectorize().join("\r\n"),
            ),
        };
        Ok(response)
    }

    pub async fn receive_clients(
        &'static self,
        cancellation_factory: impl TCancellationTokenFactory,
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
                           async move{
                                const TIMEOUT: u64 = 100;
                                let mut stream =  ClientStream(stream);
                                loop {
                                    let Ok((request, query_args)) = stream.extract_query().await else {
                                        eprintln!("invalid user request");
                                        continue;
                                    };

                                    let (cancellation_notifier, cancellation_token) =
                                        cancellation_factory.create(TIMEOUT);

                                    // TODO subject to change - more to dynamic
                                    // Notify the cancellation notifier to cancel the query after 100 milliseconds.
                                    cancellation_notifier.notify();

                                    let res = match self.handle(cancellation_token, request, query_args).await {
                                        Ok(response) => stream.write(response).await,
                                        Err(e) => stream.write(QueryIO::Err(e.to_string())).await,
                                    };

                                    if let Err(e) = res {
                                        if e.should_break() {
                                            break;
                                        }
                                    }
                                }
                           }
                        ));
                    }
                } =>{}


            _ = stop_sentinel_recv => {
                // Reconnection logic should be implemented by client?
                conn_handlers.iter().for_each(|handle| handle.abort());
                },

        };
    }
}
