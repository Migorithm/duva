pub mod cli;
pub mod command;
use clap::Parser;
use cli::Cli;
use command::build_command;
use duva::prelude::tokio::io::{AsyncReadExt, AsyncWriteExt};
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::uuid::Uuid;
use duva::prelude::*;
use duva::services::interface::TSerdeReadWrite;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    domains::query_parsers::query_io::{QueryIO, deserialize},
};
use rustyline::DefaultEditor;

async fn send_command(stream: &mut TcpStream, command: String) -> Result<(), String> {
    // TODO input validation required otherwise, it hangs
    stream.write_all(command.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut response = vec![];
    match stream.read_buf(&mut response).await {
        Ok(0) => return Err("Connection closed by server".to_string()),
        Ok(_) => {},
        Err(e) => return Err(format!("Failed to read response: {}", e)),
    };

    // Deserialize response and check if it follows RESP protocol
    match deserialize(BytesMut::from_iter(response)) {
        Ok((query_io, _)) => {
            match query_io {
                QueryIO::BulkString(value) => {
                    println!("{}", value);
                    Ok(())
                },
                QueryIO::Err(err) => {
                    println!("{}", err);
                    Ok(()) // We still return Ok since we got a valid RESP error response
                },
                QueryIO::Null => {
                    println!("(nil)");
                    Ok(())
                },
                QueryIO::SimpleString(val) => {
                    println!("{}", val);
                    Ok(())
                },
                _ => {
                    let err_msg = "Unexpected response format";
                    println!("{}", err_msg);
                    Err(err_msg.to_string())
                },
            }
        },
        Err(e) => {
            let err_msg = format!("Invalid RESP protocol: {}", e);
            println!("{}", err_msg);
            Err(err_msg)
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let mut rl = DefaultEditor::new().expect("Failed to initialize input editor");
    let mut stream = TcpStream::connect(&cli.address()).await.unwrap();

    stream.ser_write(AuthRequest::ConnectWithoutId).await.unwrap(); // client_id not exist

    let AuthResponse::ClientId(client_id) = stream.de_read().await.unwrap();
    let client_id = Uuid::parse_str(&client_id).unwrap();
    println!("Client ID: {}", client_id);
    println!("Connected to Redis at {}", cli.address());

    loop {
        let readline = rl.readline("duva-cli> ").map_err(|e| "Failed to read line".to_string())?;

        let args: Vec<&str> = readline.split_whitespace().collect();
        if args.is_empty() {
            continue;
        }
        if args[0].eq_ignore_ascii_case("exit") {
            println!("Exiting...");
            break;
        }

        match build_command(args) {
            Ok(command) => {
                if let Err(e) = send_command(&mut stream, command).await {
                    println!("{}", e);
                }
            },
            Err(e) => {
                println!("{}", e);
                continue;
            },
        }
    }
    Ok(())
}
