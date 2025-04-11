use crate::command::ClientInputKind;
use duva::prelude::PeerIdentifier;
use duva::prelude::tokio;
use duva::prelude::tokio::io::AsyncWriteExt;
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::net::tcp::OwnedWriteHalf;
use duva::prelude::tokio::sync::mpsc::Receiver;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::tokio::time::timeout;
use std::time::Duration;

use duva::prelude::tokio::sync::oneshot;

use duva::prelude::uuid::Uuid;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    services::interface::{TRead, TSerdeReadWrite},
};
use duva::{
    domains::{IoError, query_parsers::query_io::QueryIO},
    prelude::tokio::net::tcp::OwnedReadHalf,
};

// TODO Read actor and Write actor
pub struct ClientController<T> {
    read_kill_switch: Option<tokio::sync::oneshot::Sender<()>>,
    from_server: Receiver<Readable>,
    to_server: Sender<Sendable>,
    client_id: Uuid,
    request_id: u64,
    latest_known_index: u64,
    cluster_nodes: Vec<PeerIdentifier>,
    pub target: T,
}

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &str) -> Self {
        let (r, w, mut auth_response) =
            ClientController::<T>::authenticate(server_addr, None).await.unwrap();

        auth_response.cluster_nodes.push(server_addr.to_string().into());
        let (tx, rx) = tokio::sync::mpsc::channel::<Readable>(100);

        Self {
            read_kill_switch: Some(r.run(tx)),
            from_server: rx,
            to_server: w.run(),
            client_id: Uuid::parse_str(&auth_response.client_id).unwrap(),
            target: editor,
            latest_known_index: 0,
            request_id: auth_response.request_id,
            cluster_nodes: auth_response.cluster_nodes,
        }
    }

    async fn authenticate(
        server_addr: &str,
        auth_request: Option<AuthRequest>,
    ) -> Result<(ServerStreamReader, ServerStreamWriter, AuthResponse), IoError> {
        let mut stream =
            TcpStream::connect(server_addr).await.map_err(|e| IoError::ConnectionRefused)?;
        stream.serialized_write(auth_request.unwrap_or(AuthRequest::default())).await.unwrap(); // client_id not exist

        let auth_response: AuthResponse = stream.deserialized_read().await?;
        let (r, w) = stream.into_split();
        Ok((ServerStreamReader(r), ServerStreamWriter(w), auth_response))
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
    ) -> Result<(), IoError> {
        //TODO separate out listening
        //TODO PAIN : how do we map input with command?
        if let Ok(Some(Readable::FromServer(Ok(QueryIO::TopologyChange(cluster_nodes))))) =
            timeout(Duration::from_millis(10), self.from_server.recv()).await
        {
            self.cluster_nodes = cluster_nodes;
        }

        self.to_server.send(Sendable::Command(command.as_bytes().to_vec())).await.unwrap();

        match self.from_server.recv().await.unwrap() {
            Readable::FromServer(Ok(query_io)) => {
                self.may_update_request_id(&input);
                self.render_return_per_input(input, query_io).map_err(|e| IoError::Custom(e))?;
            },
            Readable::FromServer(Err(e)) => {
                println!("Error: {}", e);
                match e {
                    IoError::ConnectionAborted | IoError::ConnectionReset => {
                        self.discover_leader().await?;
                        // recursively call the function to retry
                        Box::pin(self.send_command(command, input)).await?;
                        return Ok(());
                    },
                    _ => {
                        return Err(e);
                    },
                }
            },
        }

        Ok(())
    }

    // pull-based leader discovery
    async fn discover_leader(&mut self) -> Result<(), IoError> {
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
                self.replace_stream(r, w).await;

                self.cluster_nodes = auth_response.cluster_nodes;

                return Ok(());
            }
        }
        Err(IoError::Custom("No leader found in the cluster".to_string()))
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

    async fn replace_stream(&mut self, r: ServerStreamReader, w: ServerStreamWriter) {
        self.read_kill_switch.take().unwrap().send(()).unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<Readable>(100);
        self.read_kill_switch = Some(r.run(tx));
        self.to_server.send(Sendable::Stop).await.unwrap();
        self.to_server = w.run();
        self.from_server = rx;
    }
}

pub struct ServerStreamReader(OwnedReadHalf);
impl ServerStreamReader {
    pub fn run(
        mut self,
        controller_sender: tokio::sync::mpsc::Sender<Readable>,
    ) -> oneshot::Sender<()> {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel::<()>();

        let future = async move {
            let controller_sender = controller_sender.clone();
            loop {
                match self.0.read_values().await {
                    Ok(query_ios) => {
                        for query_io in query_ios {
                            if controller_sender
                                .send(Readable::FromServer(Ok(query_io)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        if controller_sender.send(Readable::FromServer(Err(e))).await.is_err() {
                            break;
                        }
                    },
                }
            }
        };
        tokio::spawn(async move {
            tokio::select! {
                _ = future => {}
                _ = kill_switch => {}
            }
        });
        kill_trigger
    }
}
pub enum Readable {
    FromServer(Result<QueryIO, IoError>),
}

pub struct ServerStreamWriter(OwnedWriteHalf);

impl ServerStreamWriter {
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), String> {
        if let Err(e) = self.0.write_all(buf).await {
            return Err(format!("Failed to send command: {}", e));
        }

        if let Err(e) = self.0.flush().await {
            return Err(format!("Failed to flush stream: {}", e));
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), String> {
        if let Err(e) = self.0.flush().await {
            return Err(format!("Failed to flush stream: {}", e));
        }
        Ok(())
    }

    pub fn run(mut self) -> tokio::sync::mpsc::Sender<Sendable> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Sendable>(100);

        tokio::spawn(async move {
            while let Some(sendable) = rx.recv().await {
                match sendable {
                    Sendable::Command(cmd) => {
                        if let Err(e) = self.write_all(&cmd).await {
                            println!("{e}");
                        };
                    },
                    Sendable::Stop => break,
                }
            }
        });
        tx
    }
}

pub enum Sendable {
    Command(Vec<u8>),
    Stop,
}
