pub mod adapters;
pub mod macros;
pub mod services;
use crate::services::query_manager::client_request_controllers::ClientRequestController;
use anyhow::Result;
use services::config::config_actor::Config;
use services::config::config_manager::ConfigManager;

use services::query_manager::interface::{
    TCancellationTokenFactory, TConnectStream, TCreateStreamListener, TListenStream,
};
use services::query_manager::replication_request_controllers::ReplicationRequestController;
use services::query_manager::QueryManager;
use services::statefuls::cache::cache_manager::CacheManager;
use services::statefuls::cache::ttl_manager::TtlSchedulerInbox;
use services::statefuls::persist::persist_actor::PersistActor;

pub async fn start_up<T: TCancellationTokenFactory, L: TCreateStreamListener, C: TConnectStream>(
    config: Config,
    startup_notifier: impl TNotifyStartUp,
) -> Result<()> {
    let replication_stream_listener = L::create_listner(config.replication_bind_addr()).await;
    let client_stream_listener = L::create_listner(config.bind_addr()).await;

    let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors();

    if let Some(filepath) = config.try_filepath().await {
        let dump = PersistActor::dump(filepath).await?;
        cache_manager
            .dump_cache(dump, ttl_inbox.clone(), config.startup_time)
            .await?;
    }

    // Run Replication manager
    let config_manager = ConfigManager::run_actor(config);

    // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
    // will live for the entire duration of the program.
    let cache_manager: &'static CacheManager = Box::leak(Box::new(cache_manager));

    tokio::spawn(start_accepting_peer_connections::<C>(
        replication_stream_listener,
        config_manager.clone(),
    ));
    startup_notifier.notify_startup();

    start_accepting_client_connections::<T>(
        client_stream_listener,
        cache_manager,
        ttl_inbox,
        config_manager,
    )
    .await;
    Ok(())
}

// TODO should replica be able to receive replica traffics directly?
async fn start_accepting_peer_connections<C: TConnectStream>(
    replication_listener: impl TListenStream,
    config_manager: ConfigManager,
) {
    let replication_request_controller: &'static ReplicationRequestController =
        Box::leak(ReplicationRequestController::new(config_manager.clone()).into());

    loop {
        match replication_listener.accept().await {
            Ok((peer_stream, _)) => {
                tokio::spawn(QueryManager::handle_peer_stream::<C>(
                    peer_stream,
                    replication_request_controller,
                ));
            }

            Err(err) => {
                if err.should_break() {
                    break;
                }
            }
        }
    }
}

// TODO should replica be able to receive client traffics directly?
async fn start_accepting_client_connections<T: TCancellationTokenFactory>(
    client_stream_listener: impl TListenStream,
    cache_manager: &'static CacheManager,
    ttl_inbox: TtlSchedulerInbox,
    config_manager: ConfigManager,
) {
    // SAFETY: The client_request_controller is leaked to make it static.
    // This is safe because the client_request_controller will live for the entire duration of the program.
    let client_request_controller: &'static ClientRequestController =
        Box::leak(ClientRequestController::new(config_manager, cache_manager, ttl_inbox).into());

    loop {
        match client_stream_listener.accept().await {
            Ok((stream, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                tokio::spawn(QueryManager::handle_single_client_stream::<
                    T,
                    tokio::fs::File,
                >(stream, client_request_controller));
            }
            Err(e) => {
                if e.should_break() {
                    break;
                }
            }
        }
    }
}

pub trait TNotifyStartUp {
    fn notify_startup(&self);
}

impl TNotifyStartUp for () {
    fn notify_startup(&self) {}
}
