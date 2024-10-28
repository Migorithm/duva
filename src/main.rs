pub mod adapters;

mod config;
pub mod services;
use adapters::in_memory::InMemoryDb;
use anyhow::Result;
use config::Config;
use services::{
    config_handler::ConfigHandler,
    persistence::{persist_actor, PersistEnum},
    query_manager::{value::TtlCommand, MessageParser},
    ttl_handlers::{delete::delete_actor, set::set_ttl_actor},
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

    tokio::spawn(delete_actor(InMemoryDb));

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(set_ttl_actor(rx));

    let mut persistence_senders = Vec::with_capacity(NUM_OF_PERSISTENCE);
    (0..NUM_OF_PERSISTENCE).for_each(|_| {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(persist_actor(rx));
        persistence_senders.push(tx);
    });

    let config = Arc::new(Config::new());
    let listener = TcpListener::bind(config.bind_addr()).await?;
    loop {
        let conf = Arc::clone(&config);
        let ttl_sender = tx.clone();
        let ph = persistence_senders.clone();
        match listener.accept().await {
            Ok((socket, _)) => {
                // Spawn a new task to handle the connection without blocking the main thread.
                tokio::spawn(async move {
                    match process(socket, conf, ttl_sender, ph).await {
                        Ok(_) => println!("Connection closed"),
                        Err(e) => eprintln!("Error: {:?}", e),
                    };
                });
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn process(
    stream: TcpStream,
    conf: Arc<Config>,
    ttl_sender: Sender<TtlCommand>,
    persistence_senders: Vec<Sender<PersistEnum>>,
) -> Result<()> {
    let mut parser = MessageParser::new(stream);
    let mut handler =
        services::ServiceFacade::new(ConfigHandler::new(Arc::clone(&conf)), ttl_sender);

    loop {
        handler.handle(&mut parser, &persistence_senders).await?;
    }
}
