pub mod adapters;
pub mod config;
pub mod macros;
pub mod services;
use anyhow::Result;
use config::Config;
use services::{
    interfaces::endec::TEnDecoder,
    query_manager::QueryManager,
    statefuls::routers::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
};
use tokio::net::{TcpListener, TcpStream};

// facade for the start_up function
pub async fn start_up(
    config: &'static Config,
    number_of_cache_actors: usize,
    endec: impl TEnDecoder,
    startup_notifier: impl TNotifyStartUp,
) -> Result<()> {
    let (cache_dispatcher, ttl_inbox) =
        CacheManager::run_cache_actors(number_of_cache_actors, endec);

    cache_dispatcher
        .load_data(ttl_inbox.clone(), config)
        .await?;

    let listener = TcpListener::bind(config.bind_addr()).await?;

    startup_notifier.notify_startup();

    start_accepting_connections(listener, config, ttl_inbox, cache_dispatcher).await
}

async fn start_accepting_connections(
    listener: TcpListener,
    config: &'static Config,
    ttl_inbox: TtlSchedulerInbox,
    cache_dispatcher: CacheManager<impl TEnDecoder>,
) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((socket, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                let query_manager = QueryManager::new(socket, config);
                process_socket(query_manager, ttl_inbox.clone(), cache_dispatcher.clone())
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn process_socket<T: TEnDecoder>(
    mut query_manager: QueryManager<TcpStream>,
    ttl_inbox: TtlSchedulerInbox,
    cache_dispatcher: CacheManager<T>,
) {
    tokio::spawn(async move {
        loop {
            if let Err(e) = query_manager
                .handle(&cache_dispatcher, ttl_inbox.clone())
                .await
            {
                eprintln!("Error: {:?}", e);
                println!("Connection closed");
                break;
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
