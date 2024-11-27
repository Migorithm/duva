pub mod adapters;
mod config;
pub mod macros;
pub mod services;
use adapters::persistence::decoder::Decoder;
use anyhow::Result;
use services::{
    query_manager::QueryManager,
    statefuls::routers::{cache_manager::CacheManager, ttl_manager::TtlSchedulerInbox},
};
use std::time::SystemTime;
use tokio::net::{TcpListener, TcpStream};
#[cfg(test)]
mod test;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb
const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> Result<()> {
    let (cache_dispatcher, ttl_inbox) = CacheManager::run_cache_actors(NUM_OF_PERSISTENCE, Decoder);

    // Load data from the file if --dir and --dbfilename are provided
    cache_dispatcher
        .load_data(ttl_inbox.clone(), SystemTime::now())
        .await?;

    let listener = TcpListener::bind(config::config().bind_addr()).await?;
    loop {
        match listener.accept().await {
            Ok((socket, _)) =>
            // Spawn a new task to handle the connection without blocking the main thread.
            {
                process(socket, ttl_inbox.clone(), cache_dispatcher.clone())
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn process(
    stream: TcpStream,
    ttl_inbox: TtlSchedulerInbox,
    cache_dispatcher: CacheManager<Decoder>,
) {
    tokio::spawn(async move {
        let mut query_manager = QueryManager::new(stream);

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
