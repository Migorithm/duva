pub mod adapters;
pub mod config;
pub mod macros;
pub mod services;
use anyhow::Result;
use config::Config;
use services::{
    interfaces::endec::TEnDecoder,
    query_manager::{
        interface::{TCancelNotifier, TCancellationTokenFactory},
        QueryManager,
    },
    statefuls::routers::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
};
use std::time::SystemTime;
use tokio::net::{TcpListener, TcpStream};

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

pub async fn start_up<T: TCancellationTokenFactory>(
    config: &'static Config,
    number_of_cache_actors: usize,
    endec: impl TEnDecoder,
    startup_notifier: impl TNotifyStartUp,
) -> Result<()> {
    let (cache_dispatcher, ttl_inbox) =
        CacheManager::run_cache_actors(number_of_cache_actors, endec);

    // Load data from the file if --dir and --dbfilename are provided
    cache_dispatcher
        .load_data(ttl_inbox.clone(), SystemTime::now(), config)
        .await?;

    let listener = TcpListener::bind(config.bind_addr()).await?;
    startup_notifier.notify_startup();
    loop {
        match listener.accept().await {
            Ok((socket, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                let query_manager = QueryManager::new(socket, config);
                process_socket::<_, T>(query_manager, ttl_inbox.clone(), cache_dispatcher.clone());
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn process_socket<T: TEnDecoder, U: TCancellationTokenFactory>(
    mut query_manager: QueryManager<TcpStream>,
    ttl_inbox: TtlSchedulerInbox,
    cache_dispatcher: CacheManager<T>,
) {
    tokio::spawn(async move {
        loop {
            let (tx, rx) = U::create().split();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                tx.notify();
            });
            if let Err(e) = query_manager
                .handle(&cache_dispatcher, ttl_inbox.clone(), rx)
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
