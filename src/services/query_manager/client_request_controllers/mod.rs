use crate::services::config::config_manager::ConfigManager;
use crate::services::config::ConfigResponse;
use crate::services::query_manager::interface::TCancellationWatcher;
use crate::services::query_manager::query_io::QueryIO;
use crate::services::statefuls::cache::cache_manager::CacheManager;
use crate::services::statefuls::cache::ttl_manager::TtlSchedulerInbox;
use crate::services::statefuls::persist::endec::encoder::encoding_processor::EncodingProcessor;
use crate::services::statefuls::persist::save_actor::PersistActor;
use arguments::Arguments;
use client_request::ClientRequest;

use super::interface::TWriterFactory;

pub mod arguments;
pub mod client_request;

pub struct ClientRequestController {
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
        args: Arguments,
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
                let outbox = PersistActor::<EncodingProcessor<T>>::run(
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
                let rx = self.config_manager.route_get(cmd).await?;

                match rx.await? {
                    ConfigResponse::Dir(value) => QueryIO::Array(vec![
                        QueryIO::BulkString("dir".into()),
                        QueryIO::BulkString(value),
                    ]),
                    ConfigResponse::DbFileName(value) => QueryIO::BulkString(value),
                    _ => QueryIO::Err("Invalid operation".into()),
                }
            }
            ClientRequest::Delete => panic!("Not implemented"),

            ClientRequest::Info => {
                QueryIO::BulkString(self.config_manager.replication_info().await?.join("\r\n"))
            }
        };
        Ok(response)
    }
}
