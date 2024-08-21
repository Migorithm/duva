pub mod commands;
pub mod error;

use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    str::FromStr,
};

use commands::Command;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut reader = BufReader::new(&mut stream);
                let mut buf = String::new();

                reader.read_line(&mut buf).unwrap();

                match FromStr::from_str(&buf).unwrap() {
                    Command::Ping => {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
