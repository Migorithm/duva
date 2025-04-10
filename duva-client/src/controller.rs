use crate::command::ClientInputKind;
use duva::prelude::BytesMut;
use duva::prelude::PeerIdentifier;
use duva::prelude::tokio;
use duva::prelude::tokio::io::AsyncReadExt;
use duva::prelude::tokio::io::AsyncWriteExt;
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::net::tcp::OwnedWriteHalf;
use duva::prelude::uuid::Uuid;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    services::interface::{TRead, TSerdeReadWrite},
};
use duva::{
    domains::{
        IoError,
        query_parsers::query_io::{QueryIO, deserialize},
    },
    prelude::tokio::net::tcp::OwnedReadHalf,
};

// TODO Read actor and Write actor
pub struct ClientController<T> {
    r: ReadBuffer,
    w: WriteBuffer,
    client_id: Uuid,
    request_id: u64,
    latest_known_index: u64,
    cluster_nodes: Vec<PeerIdentifier>,
    pub target: T,
}

pub struct ReadBuffer(OwnedReadHalf);
pub struct WriteBuffer(OwnedWriteHalf);

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &str) -> Self {
        let (r, w, mut auth_response) =
            ClientController::<T>::authenticate(server_addr, None).await.unwrap();

        auth_response.cluster_nodes.push(server_addr.to_string().into());

        Self {
            r,
            w,
            client_id: Uuid::parse_str(&auth_response.client_id).unwrap(),
            target: editor,
            latest_known_index: 0,
            request_id: auth_response.request_id,
            cluster_nodes: auth_response.cluster_nodes,
        }
    }

    // pull-based leader discovery
    async fn discover_leader(&mut self) -> Result<(), String> {
        for node in &self.cluster_nodes {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            println!("Trying to connect to node: {}...", node);

            let auth_req = AuthRequest {
                client_id: Some(self.client_id.to_string()),
                request_id: self.request_id,
            };
            let Ok((r, w, auth_response)) =
                ClientController::<T>::authenticate(node, Some(auth_req)).await
            else {
                continue;
            };

            if auth_response.connected_to_leader {
                println!("Connected to a new leader: {}", node);
                self.r = r;
                self.w = w;
                self.cluster_nodes = auth_response.cluster_nodes;
                return Ok(());
            }
        }
        Err("No leader found in the cluster".to_string())
    }

    async fn authenticate(
        server_addr: &str,
        auth_request: Option<AuthRequest>,
    ) -> Result<(ReadBuffer, WriteBuffer, AuthResponse), IoError> {
        let mut stream =
            TcpStream::connect(server_addr).await.map_err(|e| IoError::ConnectionRefused)?;
        stream.serialized_write(auth_request.unwrap_or(AuthRequest::default())).await.unwrap(); // client_id not exist

        let auth_response: AuthResponse = stream.deserialized_read().await?;
        let (r, w) = stream.into_split();
        Ok((ReadBuffer(r), WriteBuffer(w), auth_response))
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
        if let Err(e) = self.w.0.write_all(cmd).await {
            return Err(format!("Failed to send command: {}", e));
        }

        if let Err(e) = self.w.0.flush().await {
            return Err(format!("Failed to flush stream: {}", e));
        }

        let mut response = BytesMut::with_capacity(512);

        if let Err(e) = self.r.0.read_bytes(&mut response).await {
            match e {
                IoError::ConnectionAborted | IoError::ConnectionReset => {
                    self.discover_leader().await?;

                    // recursively call the function to retry
                    return Box::pin(self.try_send_and_get(cmd)).await;
                },
                _ => {
                    // Handle other errors
                    return Err(format!("Failed to read response: {}", e));
                },
            }
        }

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
                    self.r.0.read(&mut buffer),
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
            Ping | Get | IndexGet | Echo | Config | Keys | Save | Info | ClusterForget | Role
            | ReplicaOf | ClusterInfo => match query_io {
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
