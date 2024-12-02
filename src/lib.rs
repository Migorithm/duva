pub mod adapters;
pub mod config;
pub mod macros;
pub mod services;
use anyhow::Result;
use config::Config;
use services::{
    interfaces::endec::TEnDecoder,
    query_manager::{
        interface::{TCancellationNotifier, TCancellationTokenFactory},
        QueryManager,
    },
    statefuls::routers::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
};
use tokio::net::{TcpListener, TcpStream};

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

pub async fn start_up<T: TCancellationTokenFactory>(
    config: &'static Config,
    number_of_cache_actors: usize,
    endec: impl TEnDecoder,
    startup_notifier: impl TNotifyStartUp,
) -> Result<()> {
    let (cache_manager, ttl_inbox) = CacheManager::run_cache_actors(number_of_cache_actors, endec);
    cache_manager.load_data(ttl_inbox.clone(), config).await?;

    // Leak the cache_dispatcher to make it static - this is safe because the cache_dispatcher
    // will live for the entire duration of the program.
    let cache_manager = Box::leak(Box::new(cache_manager));

    let listener = TcpListener::bind(config.bind_addr()).await?;

    startup_notifier.notify_startup();

    start_accepting_connections::<T>(listener, config, ttl_inbox, cache_manager).await
}

async fn start_accepting_connections<T: TCancellationTokenFactory>(
    listener: TcpListener,
    config: &'static Config,
    ttl_inbox: TtlSchedulerInbox,
    cache_dispatcher: &'static CacheManager<impl TEnDecoder>,
) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                process_socket::<_, T>(stream, ttl_inbox.clone(), cache_dispatcher, config);
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn process_socket<T: TEnDecoder, U: TCancellationTokenFactory>(
    socket: TcpStream,
    ttl_inbox: TtlSchedulerInbox,
    cache_manager: &'static CacheManager<T>,
    config: &'static Config,
) {
    let mut query_manager = QueryManager::new(socket, config, &cache_manager);

    tokio::spawn(async move {
        loop {
            let Ok(Some((cmd, args))) = query_manager.read_value().await else {
                eprintln!("invalid value given!");
                break;
            };

            let (cancellation_notifier, cancellation_watcher) = U::create().split();

            // TODO subject to change - more to dynamic
            // Notify the cancellation notifier to cancel the query after 100 milliseconds.
            cancellation_notifier.notify(100);

            if let Err(e) = query_manager
                .handle(ttl_inbox.clone(), cancellation_watcher, cmd, args)
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
