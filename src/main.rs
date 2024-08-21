pub mod commands;
pub mod error;

use std::{
    io::{Read, Write},
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

                let mut buf = vec![];

                let _ = stream.read(&mut buf).unwrap();

                let command = commands::Command::try_from(&buf[..]);
                match command {
                    Ok(commands::Command::Ping) => {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                    Err(_err) => {}
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
