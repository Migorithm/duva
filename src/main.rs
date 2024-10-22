pub mod adapters;
pub mod handlers;
pub mod interface;
pub mod protocol;

use adapters::in_memory::InMemoryDb;
use handlers::Handler;
use protocol::{command::Args, value::Value};
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;

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
    let mut handler = protocol::RespHandler::new(stream);

    loop {
        let Some(v) = handler.read_operation().await? else {
            break Err(anyhow::anyhow!("Connection closed"));
        };

        let (command, args) = Args::extract_command(v)?;

        let response = match command.as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first()?,
            "set" => Handler::handle_set(&args, InMemoryDb).await?,
            "get" => Handler::handle_get(&args, InMemoryDb).await?,
            // modify we have to add a new command
            c => panic!("Cannot handle command {}", c),
        };
        println!("Response: {:?}", response);
        handler.write_value(response).await?;
    }
}
