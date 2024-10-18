pub mod commands;
pub mod error;

use std::{
    io::{BufRead, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                // buffered reader
                let mut reader = std::io::BufReader::new(&stream);

                let mut buf = String::new();

                let _ = reader.read_line(&mut buf).unwrap();

                loop {
                    let mut buf = String::new();

                    // Read the next line
                    match reader.read_line(&mut buf) {
                        Ok(0) => {
                            // Connection was closed by the client
                            println!("Client disconnected");
                            break;
                        }
                        Ok(_) => {
                            // Process the command
                            let cmd_str = buf.trim_start_matches("");
                            let command = commands::Command::try_from(buf.as_str());
                            match command {
                                Ok(commands::Command::Ping) => {
                                    if let Err(e) = (&stream).write_all(b"+PONG\r\n") {
                                        println!("Error writing to client: {}", e);
                                        break;
                                    }
                                }
                                Err(err) => {
                                    println!("Invalid command: {:?}", err);
                                    // Optionally send an error response to the client
                                    if let Err(e) = (&stream).write_all(b"-ERR Invalid command\r\n")
                                    {
                                        println!("Error writing to client: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error reading from client: {}", e);
                            break;
                        }
                    }

                    // Ensure the response is sent immediately
                    if let Err(e) = (&stream).flush() {
                        println!("Error flushing stream: {}", e);
                        break;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
