mod input_queue;
mod read_stream;
mod write_stream;
use std::collections::HashMap;

use crate::command::Input;

use duva::domains::caches::cache_manager::IndexedValueCodec;
use duva::domains::{IoError, query_io::QueryIO};
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::sync::mpsc::Receiver;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::uuid::Uuid;
use duva::prelude::{LEADER_HEARTBEAT_INTERVAL_MAX, Topology};
use duva::prelude::{PeerIdentifier, tokio};
use duva::presentation::clients::request::ClientAction;
use duva::{
    domains::TSerdeReadWrite,
    prelude::{AuthRequest, AuthResponse},
};
use input_queue::InputQueue;
use read_stream::ServerStreamReader;
use write_stream::MsgToServer;
use write_stream::ServerStreamWriter;

pub struct Broker {
    pub(crate) tx: Sender<BrokerMessage>,
    pub(crate) rx: Receiver<BrokerMessage>,
    pub(crate) to_server: Sender<MsgToServer>,
    pub(crate) client_id: Uuid,
    pub(crate) request_id: u64,
    pub(crate) topology: Topology,
    pub(crate) leader_connections:
        HashMap<PeerIdentifier, (ServerStreamReader, ServerStreamWriter)>,
    pub(crate) read_kill_switch: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Broker {
    pub(crate) async fn run(mut self) {
        let mut queue = InputQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                | BrokerMessage::FromServer(Ok(QueryIO::TopologyChange(topology))) => {
                    self.topology = topology;
                    self.generate_leader_connections().await;
                },

                | BrokerMessage::FromServer(Ok(query_io)) => {
                    let Some(input) = queue.pop() else {
                        continue;
                    };

                    if let Some(index) = self.extract_req_id(&input.kind, &query_io) {
                        self.request_id = index;
                    };

                    input.callback.send((input.kind, query_io)).unwrap_or_else(|_| {
                        println!("Failed to send response to input callback");
                    });
                },
                | BrokerMessage::FromServer(Err(e)) => match e {
                    | IoError::ConnectionAborted | IoError::ConnectionReset => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            LEADER_HEARTBEAT_INTERVAL_MAX,
                        ))
                        .await;
                        self.discover_leader().await.unwrap();
                    },
                    | _ => {},
                },
                | BrokerMessage::ToServer(command) => {
                    let cmd = self.build_command_with_request_id(&command.command, command.args);
                    if let Err(e) =
                        self.to_server.send(MsgToServer::Command(cmd.as_bytes().to_vec())).await
                    {
                        println!("Failed to send command: {e}");
                    }
                    queue.push(command.input);
                },
            }
        }
    }

    pub fn build_command_with_request_id(&self, cmd: &str, args: Vec<String>) -> String {
        // Build the valid RESP command
        let mut command =
            format!("!{}\r\n*{}\r\n${}\r\n{}\r\n", self.request_id, args.len() + 1, cmd.len(), cmd);
        for arg in args {
            command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        command
    }

    // ! CONSIDER IDEMPOTENCY RULE
    // !
    // ! If request is updating action yet receive error, we need to increase the request id
    // ! otherwise, server will not be able to process the next command
    fn extract_req_id(&mut self, kind: &ClientAction, query_io: &QueryIO) -> Option<u64> {
        if !kind.consensus_required() {
            return None;
        }
        match query_io {
            // * Current rule: s:value-idx:index_num
            | QueryIO::SimpleString(v) => {
                let s = String::from_utf8_lossy(v);
                IndexedValueCodec::decode_index(s).filter(|&id| id > self.request_id)
            },
            | QueryIO::Err(_) => Some(self.request_id + 1),
            | _ => None,
        }
    }

    pub(crate) async fn authenticate(
        server_addr: &str,
        auth_request: Option<AuthRequest>,
    ) -> Result<(ServerStreamReader, ServerStreamWriter, AuthResponse), IoError> {
        let mut stream =
            TcpStream::connect(server_addr).await.map_err(|_| IoError::NotConnected)?;

        stream.serialized_write(auth_request.unwrap_or_default()).await.unwrap(); // client_id not exist
        let auth_response: AuthResponse = stream.deserialized_read().await?;
        let (r, w) = stream.into_split();
        Ok((ServerStreamReader(r), ServerStreamWriter(w), auth_response))
    }

    async fn replace_stream(&mut self, r: ServerStreamReader, w: ServerStreamWriter) {
        if let Some(switch) = self.read_kill_switch.take() {
            let _ = switch.send(());
        }
        self.read_kill_switch = Some(r.run(self.tx.clone()));
        self.to_server.send(MsgToServer::Stop).await.unwrap();
        self.to_server = w.run();
    }

    // pull-based leader discovery

    async fn discover_leader(&mut self) -> Result<(), IoError> {
        for node in self.topology.node_infos.iter().map(|n| n.peer_id.clone()) {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            println!("Trying to connect to node: {node}...");

            let auth_req = AuthRequest {
                client_id: Some(self.client_id.to_string()),
                request_id: self.request_id,
            };
            let Ok((r, w, auth_response)) = Self::authenticate(&node, Some(auth_req)).await else {
                continue;
            };

            if auth_response.connected_to_leader {
                println!("Connected to a new leader: {node}");
                self.replace_stream(r, w).await;
                self.topology = auth_response.topology;

                return Ok(());
            }
        }
        Err(IoError::Custom("No leader found in the cluster".to_string()))
    }

    async fn generate_leader_connections(&mut self) {
        for node in &self.topology.connected_peers {
            if self.leader_connections.contains_key(node) {
                continue;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            println!("Trying to connect to node: {}...", node);

            let auth_req = AuthRequest {
                client_id: Some(self.client_id.to_string()),
                request_id: self.request_id,
            };
            let Ok((r, w, auth_response)) = Self::authenticate(node, Some(auth_req)).await else {
                continue;
            };
            self.leader_connections.insert(node.clone(), (r, w));

            if auth_response.connected_to_leader {
                println!("Connected to leader: {}", node);
            }
        }
    }
}

pub enum BrokerMessage {
    FromServer(Result<QueryIO, IoError>),
    ToServer(CommandToServer),
}
impl BrokerMessage {
    pub fn from_command(command: String, args: Vec<&str>, input: Input) -> Self {
        BrokerMessage::ToServer(CommandToServer {
            command,
            args: args.iter().map(|s| s.to_string()).collect(),
            input,
        })
    }
}

pub struct CommandToServer {
    pub command: String,
    pub args: Vec<String>,
    pub input: Input,
}
