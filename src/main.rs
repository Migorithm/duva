pub mod adapters;
mod config;
pub mod services;
use adapters::in_memory::InMemoryDb;
use anyhow::Result;
use config::Config;
use services::{
    config_handler::ConfigHandler, parser::MessageParser, persistence_handler::PersistenceHandler,
};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

#[cfg(test)]
mod test;

/// dir, dbfilename is given as follows: ./your_program.sh --dir /tmp/redis-files --dbfilename dump.rdb

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let config = Arc::new(Config::new());

    let listener = TcpListener::bind(config.bind_addr()).await?;
    loop {
        let conf = Arc::clone(&config);
        match listener.accept().await {
            Ok((socket, _)) => {
                // Spawn a new task to handle the connection without blocking the main thread.
                tokio::spawn(async move {
                    match process(socket, conf).await {
                        Ok(_) => println!("Connection closed"),
                        Err(e) => eprintln!("Error: {:?}", e),
                    };
                });
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn process(stream: TcpStream, conf: Arc<Config>) -> Result<()> {
    let mut parser = MessageParser::new(stream);
    let mut handler = services::ServiceFacade::new(
        ConfigHandler::new(Arc::clone(&conf)),
        PersistenceHandler::new(InMemoryDb),
    );

    loop {
        handler.handle(&mut parser).await?;
    }
}
