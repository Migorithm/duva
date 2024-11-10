pub mod adapters;
mod config;

pub mod macros;
pub mod services;

use adapters::controller::Controller;
use anyhow::Result;
use config::Config;
use services::{
    config_handler::ConfigHandler,
    statefuls::{
        routers::inmemory_router::{run_cache_actors, CacheDbMessageRouter},
        ttl_handlers::{
            delete::run_delete_expired_key_actor,
            set::{run_set_ttl_actor, TtlSetter},
        },
    },
};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

#[cfg(test)]
mod test;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.

    let persistence_senders = run_cache_actors(NUM_OF_PERSISTENCE);
    run_delete_expired_key_actor(persistence_senders.clone());
    let ttl_sender = run_set_ttl_actor();

    let config = Arc::new(Config::new());
    let listener = TcpListener::bind(config.bind_addr()).await?;
    loop {
        let conf = Arc::clone(&config);
        let t_sender = ttl_sender.clone();
        let ph = persistence_senders.clone();
        match listener.accept().await {
            Ok((socket, _)) => {
                // Spawn a new task to handle the connection without blocking the main thread.
                process(socket, conf, t_sender, ph)
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

fn process(
    stream: TcpStream,
    conf: Arc<Config>,
    ttl_sender: TtlSetter,
    persistence_router: CacheDbMessageRouter,
) {
    tokio::spawn(async move {
        let mut io_controller = Controller::new(stream);

        let config_handler = ConfigHandler::new(Arc::clone(&conf));

        loop {
            match io_controller
                .handle(
                    &persistence_router,
                    ttl_sender.clone(),
                    config_handler.clone(),
                )
                .await
            {
                Ok(_) => println!("Connection closed"),
                Err(e) => eprintln!("Error: {:?}", e),
            }
        }
    });
}
