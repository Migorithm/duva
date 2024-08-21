pub mod commands;
pub mod error;

use std::{
    io::{BufRead, Read, Write},
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

                let command = commands::Command::try_from(buf.as_bytes());
                match command {
                    Ok(commands::Command::Ping) => {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                    Err(_err) => {
                        println!("arrived?")
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
