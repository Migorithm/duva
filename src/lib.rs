pub mod adapters;
pub mod config;
pub mod macros;

pub mod services;

use anyhow::Result;
use config::Config;
use services::{
    interfaces::endec::{TDecodeData, TEncodeData},
    query_manager::QueryManager,
    statefuls::routers::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
};
use std::time::SystemTime;
use tokio::net::{TcpListener, TcpStream};
#[cfg(test)]
mod test;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

pub async fn start_up(
    config: &'static Config,
    number_of_cache_actors: usize,
    endec: impl TDecodeData + TEncodeData,
) -> Result<()> {
    let (cache_dispatcher, ttl_inbox) =
        CacheManager::run_cache_actors(number_of_cache_actors, endec.clone());

    // Load data from the file if --dir and --dbfilename are provided
    cache_dispatcher
        .load_data(ttl_inbox.clone(), SystemTime::now(), config)
        .await?;

    let listener = TcpListener::bind(config.bind_addr()).await?;
    loop {
        match listener.accept().await {
            Ok((socket, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                let query_manager = QueryManager::new(socket, config, endec.clone());
                process_socket(query_manager, ttl_inbox.clone(), cache_dispatcher.clone())
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn process_socket<T: TDecodeData + TEncodeData>(
    mut query_manager: QueryManager<TcpStream, T>,
    ttl_inbox: TtlSchedulerInbox,
    cache_dispatcher: CacheManager<T>,
) {
    tokio::spawn(async move {
        loop {
            match query_manager
                .handle(&cache_dispatcher, ttl_inbox.clone())
                .await
            {
                Ok(_) => println!("Connection closed"),
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    break;
                }
            }
        }
    });
}