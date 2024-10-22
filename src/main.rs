use resp::Value;
use tokio::net::{TcpListener, TcpStream};
mod resp;
use anyhow::Result;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // Spawn a new task to handle the connection without blocking the main thread.
                tokio::spawn(async move {
                    process(socket).await;
                });
            }
            Err(e) => eprintln!("Failed to accept connection: {:?}", e),
        }
    }
}

async fn process(stream: TcpStream) {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        let value = handler.read_operation().await.unwrap();
        let Some(v) = value else {
            break;
        };

        let (command, args) = extract_command(v).unwrap();

        let response = match command.as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first().unwrap().clone(),
            // modify we have to add a new command
            c => panic!("Cannot handle command {}", c),
        };
        println!("Response: {:?}", response);
        handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => Ok((
            unpack_bulk_str(a.first().unwrap().clone())?,
            a.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s.to_lowercase()),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
    }
}
