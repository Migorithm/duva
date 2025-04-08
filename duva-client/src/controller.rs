use crate::command::ClientInputKind;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    domains::query_parsers::query_io::{QueryIO, deserialize},
    prelude::{
        BytesMut, PeerIdentifier,
        tokio::{
            self,
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        },
        uuid::Uuid,
    },
    services::interface::{TRead, TSerdeReadWrite},
};

pub struct ClientController<T> {
    stream: TcpStream,
    client_id: Uuid,
    request_id: u64,
    latest_known_index: u64,
    cluster_nodes: Vec<PeerIdentifier>,
    pub target: T,
}

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &str) -> Self {
        let (stream, mut auth_response) = ClientController::<T>::authenticate(server_addr).await;
        auth_response.cluster_nodes.push(server_addr.to_string().into());
        Self {
            stream,
            client_id: Uuid::parse_str(&auth_response.client_id).unwrap(),
            target: editor,
            latest_known_index: 0,
            request_id: auth_response.request_id,
            cluster_nodes: auth_response.cluster_nodes,
        }
    }

    async fn authenticate(server_addr: &str) -> (TcpStream, AuthResponse) {
        let mut stream = TcpStream::connect(server_addr).await.unwrap();
        stream.serialized_write(AuthRequest::default()).await.unwrap(); // client_id not exist

        let auth_response: AuthResponse = stream.deserialized_read().await.unwrap();

        (stream, auth_response)
    }

    pub fn build_command(&self, cmd: &str, args: Vec<&str>) -> String {
        // Build the valid RESP command
        let mut command =
            format!("!{}\r\n*{}\r\n${}\r\n{}\r\n", self.request_id, args.len() + 1, cmd.len(), cmd);
        for arg in args {
            command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        command
    }

    pub async fn send_command(
        &mut self,
        command: String,
        input: ClientInputKind,
    ) -> Result<(), String> {
        let query_io = self.try_send_and_get(command.as_bytes()).await?;

        self.may_update_request_id(&input);
        // Deserialize response and check if it follows RESP protocol
        self.render_return_per_input(input, query_io)
    }

    async fn try_send_and_get(&mut self, cmd: &[u8]) -> Result<QueryIO, String> {
        // TODO input validation required otherwise, it hangs
        if let Err(e) = self.stream.write_all(cmd).await {
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

        Ok(query_io)
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
            Ping | Get | IndexGet | Echo | Config | Keys | Save | Info | ClusterForget
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
            Del | Exists => {
                let QueryIO::SimpleString(value) = query_io else {
                    return Err("Unexpected response format".to_string());
                };
                let deleted_count = value.parse::<u64>().unwrap();
                println!("(integer) {}", deleted_count);
            },
            Set => {
                let v = match query_io {
                    QueryIO::SimpleString(value) => value,
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
            ClientInputKind::Set | ClientInputKind::Del | ClientInputKind::Save => {
                self.request_id += 1;
            },

            _ => {},
        }
    }
}
