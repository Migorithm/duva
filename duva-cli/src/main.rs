use clap::Parser;
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

#[derive(Parser)]
#[command(name = "redis-cli", version = "1.0", about = "A simple interactive Redis CLI in Rust")]
#[clap(disable_help_flag = true)]
struct Cli {
    #[arg(short, long, default_value = "6000")]
    port: u16,
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,
}

impl Cli {
    fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn build_resp_command(args: Vec<&str>) -> Result<String, String> {
    // Check for invalid characters in command parts
    // Command-specific validation
    match args[0].to_uppercase().as_str() {
        "SET" => {
            if args.len() < 3 {
                return Err("(error) ERR wrong number of arguments for 'set' command".to_string());
            }
        },
        "GET" => {
            if args.len() != 2 {
                return Err("(error) ERR wrong number of arguments for 'get' command".to_string());
            }
        },
        "DEL" => {
            if args.len() < 2 {
                return Err("(error) ERR wrong number of arguments for 'del' command".to_string());
            }
        },
        "HSET" => {
            if args.len() < 4 || args.len() % 2 != 0 {
                return Err("(error) ERR wrong number of arguments for 'hset' command".to_string());
            }
        },
        // Add other commands as needed
        _ => {
            // For unknown commands, we can either:
            // 1. Reject them
            // return Err(format!("Unknown command: {}", args[0]));
            // 2. Or let them pass but warn
            // eprintln!("Warning: Unknown command: {}", args[0]);
        },
    }

    // Build the valid RESP command
    let mut command = format!("*{}\r\n", args.len());
    for arg in args {
        command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    Ok(command)
}

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
async fn main() {
    let cli = Cli::parse();
    let mut rl = DefaultEditor::new().expect("Failed to initialize input editor");
    let mut stream = TcpStream::connect(&cli.address()).await.unwrap();

    stream.ser_write(AuthRequest::ConnectWithoutId).await.unwrap(); // client_id not exist

    let AuthResponse::ClientId(client_id) = stream.de_read().await.unwrap();
    let client_id = Uuid::parse_str(&client_id).unwrap();
    println!("Client ID: {}", client_id);
    println!("Connected to Redis at {}", cli.address());

    loop {
        let readline = rl.readline("duva-cli> ");
        match readline {
            Ok(line) => {
                let args: Vec<&str> = line.split_whitespace().collect();
                if args.is_empty() {
                    continue;
                }
                if args[0].eq_ignore_ascii_case("exit") {
                    println!("Exiting...");
                    break;
                }

                // input validation
                match build_resp_command(args.clone()) {
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
            },
            Err(_) => {
                println!("Exiting...");
                break;
            },
        }
    }
}
