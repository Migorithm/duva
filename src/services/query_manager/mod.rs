pub mod interface;
pub mod query_io;
pub mod request;
mod query_arguments;

use crate::{
    config::Config,
    services::statefuls::cache::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
};
use anyhow::Result;
use bytes::BytesMut;
use interface::{TRead, TWrite};
use query_io::QueryIO;
use request::UserRequest;

use super::statefuls::persist::{endec::TEnDecoder, save_actor::SaveActor};
use query_arguments::QueryArguments;
use std::str::FromStr;

/// Controller is a struct that will be used to read and write values to the client.
pub struct QueryManager<T, U>
where
    T: TWrite + TRead,
    U: TEnDecoder,
{
    pub(crate) stream: T,
    config: &'static Config,
    cache_manager: &'static CacheManager<U>,
    ttl_manager: TtlSchedulerInbox,
}

impl<T, U> QueryManager<T, U>
where
    T: TWrite + TRead,
    U: TEnDecoder,
{
    pub(crate) async fn handle(
        &mut self,
        mut cancellation_token: impl interface::TCancellationWatcher,
        cmd: UserRequest,
        args: QueryArguments,
    ) -> Result<()> {
        if cancellation_token.watch() {
            self.write_value(QueryIO::Err(
                "Error opertation cancelled due to timeout".to_string(),
            ))
            .await?;
            return Ok(());
        }

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            UserRequest::Ping => QueryIO::SimpleString("PONG".to_string()),
            UserRequest::Echo => args.first().ok_or(anyhow::anyhow!("Not exists"))?.clone(),
            UserRequest::Set => {
                let cache_entry = args.take_set_args()?;
                self.cache_manager
                    .route_set(cache_entry, self.ttl_manager.clone())
                    .await?
            }
            UserRequest::Save => {
                // spawn save actor
                let outbox = SaveActor::run(
                    self.config.get_filepath().unwrap_or("dump.rdb".into()),
                    self.cache_manager.inboxes.len(),
                    self.cache_manager.endecoder.clone(),
                );

                self.cache_manager.route_save(outbox).await;

                QueryIO::Null
            }
            UserRequest::Get => {
                let key = args.take_get_args()?;
                self.cache_manager.route_get(key).await?
            }
            UserRequest::Keys => {
                let pattern = args.take_keys_pattern()?;
                self.cache_manager.route_keys(pattern).await?
            }
            // modify we have to add a new command
            UserRequest::Config => {
                let cmd = args.take_config_args()?;
                match self.config.handle_config(cmd) {
                    Some(value) => QueryIO::Array(vec![
                        QueryIO::BulkString("dir".to_string()),
                        QueryIO::BulkString(value),
                    ]),
                    None => QueryIO::Null,
                }
            }
            UserRequest::Delete => panic!("Not implemented"),

            UserRequest::Info => {
                QueryIO::BulkString(self.config.replication_info().await.join("\r\n"))
            }
        };

        self.write_value(response).await?;
        Ok(())
    }
}

impl<T, U> QueryManager<T, U>
where
    T: TWrite + TRead,
    U: TEnDecoder,
{
    pub(crate) fn new(
        stream: T,
        config: &'static Config,
        cache_manager: &'static CacheManager<U>,
        ttl_manager: TtlSchedulerInbox,
    ) -> Self {
        QueryManager {
            stream,
            config,
            cache_manager,
            ttl_manager,
        }
    }

    // crlf
    pub async fn read_value(&mut self) -> Result<Option<(UserRequest, QueryArguments)>> {
        let mut buffer = BytesMut::with_capacity(512);
        self.stream.read_bytes(&mut buffer).await?;

        let (user_request, _) = query_io::parse(buffer)?;

        let (str_cmd, values) = Self::extract_query(user_request)?;

        Ok(Some((FromStr::from_str(&str_cmd)?, values)))
    }

    pub async fn write_value(&mut self, value: QueryIO) -> Result<()> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }

    pub(crate) fn extract_query(value: QueryIO) -> Result<(String, QueryArguments)> {
        match value {
            QueryIO::Array(value_array) => Ok((
                value_array.first().unwrap().clone().unpack_bulk_str()?,
                QueryArguments::new(value_array.into_iter().skip(1).collect()),
            )),
            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
}