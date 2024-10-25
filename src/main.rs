pub mod adapters;
pub mod handlers;
pub mod interface;
pub mod protocol;

use adapters::in_memory::InMemoryDb;
use handlers::Handler;
use protocol::{command::Args, value::Value, MessageParser};
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;

#[cfg(test)]
mod test;

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // Spawn a new task to handle the connection without blocking the main thread.
                tokio::spawn(async move {
                    match process(socket).await {
                        Ok(_) => println!("Connection closed"),
                        Err(e) => eprintln!("Error: {:?}", e),
                    };
                });
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn process(stream: TcpStream) -> Result<()> {
    let mut handler = protocol::MessageParser::new(stream);

    loop {
        Handler::handle(&mut handler).await?;
    }
}
