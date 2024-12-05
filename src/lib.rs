pub mod adapters;
pub mod config;
pub mod macros;
pub mod services;

use crate::services::query_manager::UserRequestHandler;
use anyhow::Result;
use config::Config;
use services::{
    query_manager::{
        interface::{TCancellationNotifier, TCancellationTokenFactory, TRead, TWrite},
        QueryManager,
    },
    statefuls::{
        cache::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
        persist::endec::TEnDecoder,
    },
};
use std::str::FromStr;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

pub async fn start_up<T: TCancellationTokenFactory>(
    config: &'static Config,
    number_of_cache_actors: usize,
    endec: impl TEnDecoder,
    startup_notifier: impl TNotifyStartUp,
    listener: impl TSocketListener,
) -> Result<()> {
    let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors(number_of_cache_actors, endec);
    cache_manager.load_data(ttl_inbox.clone(), config).await?;

    // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
    // will live for the entire duration of the program.
    let cache_manager = Box::leak(Box::new(cache_manager));

    startup_notifier.notify_startup();

    start_accepting_connections::<T>(listener, config, ttl_inbox, cache_manager).await
}

async fn start_accepting_connections<T: TCancellationTokenFactory>(
    listener: impl TSocketListener,
    config: &'static Config,
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager<impl TEnDecoder>,
) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                let query_manager =
                    QueryManager::new(stream);
                let request_handler =
                    UserRequestHandler::new(config, &cache_manager, ttl_inbox.clone());
                handle_single_user_stream::<T>(query_manager, request_handler);
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn handle_single_user_stream<U: TCancellationTokenFactory>(
    mut query_manager: QueryManager<impl TWrite + TRead>,
    mut request_handler: UserRequestHandler<impl TEnDecoder>
) {
    tokio::spawn(async move {
        loop {
            let Ok(Some((cmd, args))) = query_manager.read_value().await else {
                eprintln!("invalid value given!");
                break;
            };

            let user_request = FromStr::from_str(&cmd).unwrap();

            const TIMEOUT: u64 = 100;
            let (cancellation_notifier, cancellation_watcher) = U::create(TIMEOUT).split();

            // TODO subject to change - more to dynamic
            // Notify the cancellation notifier to cancel the query after 100 milliseconds.
            cancellation_notifier.notify();

            let result = request_handler.handle(cancellation_watcher, user_request, args).await;
            // TODO: refactoring needed
            match result {
                Ok(response) => {
                    query_manager.write_value(response).await.unwrap();
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    break;
                }
            }
        }
    });
}

pub trait TNotifyStartUp {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}

pub trait TSocketListener {
    fn accept(
        &self,
    ) -> impl std::future::Future<Output = Result<(impl TWrite + TRead, std::net::SocketAddr)>>;
}
