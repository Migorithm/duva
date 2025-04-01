use crate::{
    Cli, build_command,
    command::ClientInputKind,
    editor::{self, DuvaHinter},
};
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
use rustyline::{Editor, sqlite_history::SQLiteHistory};

pub const PROMPT: &str = "duva-cli> ";

pub(crate) struct ClientController {
    stream: TcpStream,
    client_id: Uuid,
    request_id: u64,
    latest_known_index: u64,
    pub(crate) editor: Editor<DuvaHinter, SQLiteHistory>,
}

impl ClientController {
    pub(crate) async fn new() -> Self {
        let cli: Cli = Cli::parse();
        let (stream, client_id, request_id) = ClientController::authenticate(&cli.address()).await;
        Self { stream, client_id, editor: editor::create(), latest_known_index: 0, request_id }
    }

    async fn authenticate(server_addr: &str) -> (TcpStream, Uuid, u64) {
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        stream.serialized_write(AuthRequest::default()).await.unwrap(); // client_id not exist

        let AuthResponse { client_id, request_id } = stream.deserialized_read().await.unwrap();

        let client_id = Uuid::parse_str(&client_id).unwrap();
        println!("Client ID: {}", client_id);
        println!("Connected to Redis at {}", server_addr);

        (stream, client_id, request_id)
    }

    pub(crate) async fn send_command(
        &mut self,
        action: &str,
        args: Vec<&str>,
        input: ClientInputKind,
    ) -> Result<(), String> {
        let command = build_command(action, args, self.request_id);

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

        let Ok((query_io, _)) = deserialize(BytesMut::from_iter(response)) else {
            let _ = self.drain_stream(100).await;
            return Err("Invalid RESP protocol".into());
        };

        self.may_update_request_id(&input);
        // Deserialize response and check if it follows RESP protocol
        self.render_return_per_input(input, query_io)
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

    fn render_return_per_input(
        &mut self,
        input: ClientInputKind,
        query_io: QueryIO,
    ) -> Result<(), String> {
        use ClientInputKind::*;
        match input {
            Ping | Get | IndexGet | Delete | Echo | Config | Keys | Save | Info | ClusterForget
            | ClusterInfo => match query_io {
                QueryIO::Null => println!("(nil)"),
                QueryIO::SimpleString(value) => println!("{value}"),
                QueryIO::BulkString(value) => println!("{value}"),
                QueryIO::Err(value) => {
                    return Err(format!("(error) {value}"));
                },
                _ => {
                    return Err("Unexpected response format".to_string());
                },
            },
            Set => {
                let v = match query_io {
                    QueryIO::SimpleString(value) => value,
                    QueryIO::BulkString(value) => value,
                    QueryIO::Err(value) => {
                        return Err(format!("(error) {value}"));
                    },
                    _ => {
                        return Err("Unexpected response format".to_string());
                    },
                };
                let rindex = v.split_whitespace().last().unwrap();
                self.latest_known_index = rindex.parse::<u64>().unwrap();
                println!("OK");
            },

            ClusterNodes => {
                let QueryIO::Array(value) = query_io else {
                    return Err("Unexpected response format".to_string());
                };
                for item in value {
                    let QueryIO::BulkString(value) = item else {
                        println!("Unexpected response format");
                        break;
                    };
                    println!("{value}");
                }
            },
        }
        Ok(())
    }

    fn may_update_request_id(&mut self, input: &ClientInputKind) {
        match input {
            ClientInputKind::Set | ClientInputKind::Delete | ClientInputKind::Save => {
                self.request_id += 1;
            },

            _ => {},
        }
    }
}
