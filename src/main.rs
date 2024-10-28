pub mod adapters;
mod backgrounds;
mod config;
pub mod services;
use adapters::in_memory::InMemoryDb;
use anyhow::Result;
use config::Config;
use services::{
    config_handler::ConfigHandler,
    parser::{value::TtlCommand, MessageParser},
    persistence_handler::PersistenceHandler,
};
use std::{sync::Arc, time::SystemTime};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Sender,
};

#[cfg(test)]
mod test;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    tokio::spawn(backgrounds::delete_actor(InMemoryDb));

    let (tx, rx) = tokio::sync::mpsc::channel(100);
    tokio::spawn(backgrounds::set_ttl_actor(rx));

    let config = Arc::new(Config::new());
    let listener = TcpListener::bind(config.bind_addr()).await?;
    loop {
        let conf = Arc::clone(&config);
        let ttl_sender = tx.clone();
        match listener.accept().await {
            Ok((socket, _)) => {
                // Spawn a new task to handle the connection without blocking the main thread.
                tokio::spawn(async move {
                    match process(socket, conf, ttl_sender).await {
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
) -> Result<()> {
    let mut parser = MessageParser::new(stream);
    let mut handler = services::ServiceFacade::new(
        ConfigHandler::new(Arc::clone(&conf)),
        PersistenceHandler::new(InMemoryDb, ttl_sender),
    );

    loop {
        handler.handle(&mut parser).await?;
    }
}
