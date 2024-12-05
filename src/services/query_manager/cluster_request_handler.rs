use crate::services::config::config_manager::ConfigManager;
use crate::services::query_manager::cluster_request::ClusterRequest;
use crate::services::query_manager::interface::TCancellationWatcher;
use crate::services::query_manager::query_arguments::QueryArguments;
use crate::services::query_manager::query_io::QueryIO;
use crate::services::statefuls::cache::cache_manager::CacheManager;
use crate::services::statefuls::cache::ttl_manager::TtlSchedulerInbox;
use crate::services::statefuls::persist::endec::TEnDecoder;

pub struct ClusterHandler<U>
where
    U: TEnDecoder,
{
    // Maybe not all of these fields are needed
    config_manager: ConfigManager,
    cache_manager: &'static CacheManager<U>,
    ttl_manager: TtlSchedulerInbox,
}

impl<U> ClusterHandler<U>
where
    U: TEnDecoder,
{
    pub(crate) fn new(
        config_manager: ConfigManager,
        cache_manager: &'static CacheManager<U>,
        ttl_manager: TtlSchedulerInbox,
    ) -> Self {
        ClusterHandler {
            config_manager,
            cache_manager,
            ttl_manager,
        }
    }

    pub(crate) async fn handle(
        &mut self,
        mut cancellation_token: impl TCancellationWatcher,
        cmd: ClusterRequest,
        args: QueryArguments,
    ) -> anyhow::Result<QueryIO> {
        if cancellation_token.watch() {
            let err = QueryIO::Err(
                "Error operation cancelled due to timeout".to_string(),
            );
            return Ok(err);
        }

        // TODO if it is persistence operation, get the key and hash, take the appropriate sender, send it;
        let response = match cmd {
            ClusterRequest::Ping => QueryIO::SimpleString("PONG".to_string()),
        };
        Ok(response)
    }
}