mod config;
pub mod services;

use anyhow::Result;
use config::Config;
use services::{
    config_handler::ConfigHandler,
    persistence::{run_persistent_actors, PersistenceRouter},
    query_manager::{value::TtlCommand, MessageParser},
    ttl_handlers::{delete::run_delete_expired_key_actor, set::run_set_ttl_actor},
};
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Sender,
};

#[cfg(test)]
mod test;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let persistence_senders = run_persistent_actors(NUM_OF_PERSISTENCE);
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
    ttl_sender: Sender<TtlCommand>,
    persistence_router: PersistenceRouter,
) {
    tokio::spawn(async move {
        let mut parser = MessageParser::new(stream);
        let mut handler =
            services::ServiceFacade::new(ConfigHandler::new(Arc::clone(&conf)), ttl_sender);

        loop {
            match handler.handle(&mut parser, &persistence_router).await {
                Ok(_) => println!("Connection closed"),
                Err(e) => eprintln!("Error: {:?}", e),
            }
        }
    });
}
