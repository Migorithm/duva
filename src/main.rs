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

                let commands = buf
                    .trim_start_matches(r#"*1\r\n$4\r\n"#)
                    .trim_end_matches("\r\n")
                    .split("\\n")
                    .flat_map(|st| {
                        let cmd_str = st.trim_start_matches("\\r").trim_end_matches("\\r");
                        if !cmd_str.is_empty() {
                            Some(
                                commands::Command::try_from(
                                    st.trim_start_matches("\\r").trim_end_matches("\\r"),
                                )
                                .unwrap(),
                            )
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                for cmd in commands {
                    match cmd {
                        commands::Command::Ping => {
                            let _ = stream.write_all(b"+PONG\r\n");
                        }
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
