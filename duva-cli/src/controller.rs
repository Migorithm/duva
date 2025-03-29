use crate::{Cli, build_command};
use clap::Parser;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    domains::query_parsers::query_io::{QueryIO, deserialize},
    prelude::{
        BytesMut,
        tokio::{
            self,
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        },
        uuid::Uuid,
    },
    services::interface::{TRead, TSerdeReadWrite},
};
use rustyline::{DefaultEditor, Editor, history::FileHistory};

pub const PROMPT: &str = "duva-cli> ";

pub(crate) struct ClientController {
    stream: TcpStream,
    client_id: Uuid,
    pub(crate) editor: Editor<(), FileHistory>,
}

impl ClientController {
    pub(crate) async fn new() -> Self {
        let cli: Cli = Cli::parse();
        let editor = DefaultEditor::new().expect("Failed to initialize input editor");
        let (stream, client_id) = ClientController::authenticate(&cli.address()).await;
        Self { stream, client_id, editor }
    }

    async fn authenticate(server_addr: &str) -> (TcpStream, Uuid) {
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        stream.ser_write(AuthRequest::ConnectWithoutId).await.unwrap(); // client_id not exist

        let AuthResponse::ClientId(client_id) = stream.de_read().await.unwrap();
        let client_id = Uuid::parse_str(&client_id).unwrap();
        println!("Client ID: {}", client_id);
        println!("Connected to Redis at {}", server_addr);

        (stream, client_id)
    }

    pub(crate) async fn send_command(&mut self, args: Vec<&str>) -> Result<(), String> {
        // If previous command had a protocol error, try to recover the connection

        let command = build_command(args);

        // TODO input validation required otherwise, it hangs
        if let Err(e) = self.stream.write_all(command.as_bytes()).await {
            return Err(format!("Failed to send command: {}", e));
        }

        if let Err(e) = self.stream.flush().await {
            return Err(format!("Failed to flush stream: {}", e));
        }

        let mut response = BytesMut::with_capacity(512);
        match self.stream.read_bytes(&mut response).await {
            Ok(_) => {},
            Err(e) => return Err(format!("Failed to read response: {}", e)),
        };

        // Deserialize response and check if it follows RESP protocol
        match deserialize(BytesMut::from_iter(response)) {
            Ok((query_io, _)) => {
                match query_io {
                    QueryIO::BulkString(value) => {
                        println!("{value}");
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
                    QueryIO::SimpleString(value) => {
                        println!("{value}",);
                        Ok(())
                    },

                    QueryIO::Array(array) => {
                        for item in array {
                            let QueryIO::BulkString(value) = item else {
                                println!("Unexpected response format");
                                break;
                            };
                            println!("{value}");
                        }
                        Ok(())
                    },

                    _ => {
                        let err_msg = "Unexpected response format";
                        println!("{err_msg}");
                        Err(err_msg.to_string())
                    },
                }
            },
            Err(e) => {
                let err_msg = format!("Invalid RESP protocol: {}", e);
                println!("{err_msg}");

                // Try to recover for next command
                let _ = self.drain_stream(100).await;

                Err(err_msg)
            },
        }
    }

    async fn drain_stream(&mut self, timeout_ms: u64) -> Result<(), String> {
        // Use a timeout to avoid getting stuck
        let timeout_duration = std::time::Duration::from_millis(timeout_ms);

        let drain_future = async {
            let mut total_bytes_drained = 0;
            let mut buffer = [0u8; 1024];

            // Try to read with a very short timeout
            loop {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(10),
                    self.stream.read(&mut buffer),
                )
                .await
                {
                    Ok(Ok(0)) => break, // Connection closed
                    Ok(Ok(n)) => {
                        total_bytes_drained += n;
                        // If we've drained a lot of data, let's assume we're done
                        if total_bytes_drained > 4096 {
                            break;
                        }
                    },
                    Ok(Err(_)) | Err(_) => break, // Error or timeout
                }
            }

            Ok(())
        };

        // Apply an overall timeout to the drain operation
        match tokio::time::timeout(timeout_duration, drain_future).await {
            Ok(result) => result,
            Err(_) => Err("Timeout while draining the stream".to_string()),
        }
    }
}
