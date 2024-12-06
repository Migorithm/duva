pub mod adapters;
pub mod macros;
pub mod services;

use crate::services::query_manager::client_request_controllers::ClientRequestController;
use anyhow::Result;
use services::{
    config::{config_actor::Config, config_manager::ConfigManager},
    query_manager::{
        interface::{TCancellationNotifier, TCancellationTokenFactory, TRead, TWrite},
        QueryManager,
    },
    statefuls::{
        cache::cache_manager::CacheManager,
        persist::endec::TEnDecoder,
    },
};
use std::io::ErrorKind;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

pub async fn start_up<T: TCancellationTokenFactory>(
    config: Config,
    number_of_cache_actors: usize,
    endec: impl TEnDecoder,
    startup_notifier: impl TNotifyStartUp,
    listener: impl TSocketListener,
) -> Result<()> {
    let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors(number_of_cache_actors, endec);
    cache_manager
        .load_data(
            ttl_inbox.clone(),
            config.try_filepath().await,
            config.startup_time,
        )
        .await?;

    let config_manager = ConfigManager::run_with_config(config);

    // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
    // will live for the entire duration of the program.
    let cache_manager: &'static CacheManager<_> = Box::leak(Box::new(cache_manager));

    startup_notifier.notify_startup();

    let request_handler = ClientRequestController::new(
        config_manager,
        cache_manager,
        ttl_inbox,
    );
    start_accepting_connections::<T>(listener, request_handler).await
}

async fn start_accepting_connections<T: TCancellationTokenFactory>(
    listener: impl TSocketListener,
    handler: &'static ClientRequestController<impl TEnDecoder + Sized>,
) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                let query_manager = QueryManager::new(stream, handler);
                handle_single_user_stream::<T>(query_manager);
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn handle_single_user_stream<U: TCancellationTokenFactory>(
    mut query_manager: QueryManager<impl TWrite + TRead, &'static ClientRequestController<impl TEnDecoder>>,
) {
    tokio::spawn(async move {
        loop {
            let Ok((request, args)) = query_manager.extract_query().await else {
                eprintln!("invalid user request");
                continue;
            };

            const TIMEOUT: u64 = 100;
            let (cancellation_notifier, cancellation_watcher) = U::create(TIMEOUT).split();

            // TODO subject to change - more to dynamic
            // Notify the cancellation notifier to cancel the query after 100 milliseconds.
            cancellation_notifier.notify();

            let result = query_manager.handle(cancellation_watcher, request, args).await;
            if let Err(e) = result {
                match e.kind() {
                    ErrorKind::ConnectionRefused |
                    ErrorKind::ConnectionReset |
                    ErrorKind::NetworkUnreachable |
                    ErrorKind::ConnectionAborted |
                    ErrorKind::NotConnected |
                    ErrorKind::NetworkDown |
                    ErrorKind::BrokenPipe |
                    ErrorKind::TimedOut => {
                        eprintln!("network error: connection closed");
                        break;
                    }
                    _ => {}
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
