mod input_queue;
mod leader_connections;
mod read_stream;
mod write_stream;

use crate::command::Input;

use crate::broker::leader_connections::LeaderConnections;
use duva::domains::caches::cache_manager::IndexedValueCodec;
use duva::domains::{query_io::QueryIO, IoError};
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::sync::mpsc::Receiver;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::uuid::Uuid;
use duva::prelude::{anyhow, Topology, LEADER_HEARTBEAT_INTERVAL_MAX};
use duva::prelude::{tokio, PeerIdentifier};
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
    pub(crate) client_id: Uuid,
    pub(crate) request_id: u64,
    pub(crate) topology: Topology,
    pub(crate) leader_connections: LeaderConnections,
}

impl Broker {
    pub(crate) async fn new(server_addr: &PeerIdentifier) -> anyhow::Result<Self> {
        let (r, w, auth_response) = Broker::authenticate(&server_addr.clone(), None).await?;

        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        Ok(Broker {
            tx: broker_tx.clone(),
            rx,
            client_id: Uuid::parse_str(&auth_response.client_id)?,
            request_id: auth_response.request_id,
            topology: auth_response.topology,
            leader_connections: LeaderConnections::new(
                server_addr.clone(),
                w.run(),
                r.run_with_id(broker_tx.clone(), server_addr.clone()),
            ),
        })
    }
    pub(crate) async fn run(mut self) {
        let mut queue = InputQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                | BrokerMessage::FromServerWithId(_, Ok(QueryIO::TopologyChange(topology))) => {
                    self.topology = topology;
                    self.generate_leader_connections().await;
                },

                | BrokerMessage::FromServerWithId(_, Ok(query_io)) => {
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
                | BrokerMessage::FromServerWithId(peer_id, Err(e)) => match e {
                    | IoError::ConnectionAborted | IoError::ConnectionReset => {
                        if self.leader_connections.is_main_leader(&peer_id) {
                            // Main leader disconnected - discover new leader
                            println!(
                                "Main leader {} disconnected, discovering new leader",
                                peer_id
                            );
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                LEADER_HEARTBEAT_INTERVAL_MAX,
                            ))
                            .await;
                            self.discover_leader().await.unwrap();
                        } else {
                            // Secondary leader disconnected - just remove from connections
                            println!(
                                "Secondary leader {} disconnected, removing from connections",
                                peer_id
                            );
                            if let Some(connection) =
                                self.leader_connections.remove_connection(&peer_id)
                            {
                                let _ = connection.writer.send(MsgToServer::Stop).await;
                                let _ = connection.kill_switch.send(());
                            }
                        }
                    },
                    | _ => {},
                },
                | BrokerMessage::ToServer(command) => {
                    if let Err(err) = self.determine_route(&command).await {
                        println!("Failed to determine route for command: {:?}", err);
                    }
                    queue.push(command.input);
                },
            }
        }
    }

    pub fn build_command_with_request_id(&self, cmd: &str, args: &Vec<String>) -> String {
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
        server_addr: &PeerIdentifier,
        auth_request: Option<AuthRequest>,
    ) -> Result<(ServerStreamReader, ServerStreamWriter, AuthResponse), IoError> {
        let mut stream =
            TcpStream::connect(server_addr.as_str()).await.map_err(|_| IoError::NotConnected)?;

        stream.serialized_write(auth_request.unwrap_or_default()).await?; // client_id not exist
        let auth_response: AuthResponse = stream.deserialized_read().await?;
        let (r, w) = stream.into_split();
        Ok((ServerStreamReader(r), ServerStreamWriter(w), auth_response))
    }

    async fn replace_stream(&mut self, r: ServerStreamReader, w: ServerStreamWriter) {
        if let Some(connection) = self.leader_connections.remove_main_connection() {
            let _ = connection.writer.send(MsgToServer::Stop).await;
            let _ = connection.kill_switch.send(());
        }
        let read_kill_switch =
            r.run_with_id(self.tx.clone(), self.leader_connections.main_leader_id().clone());
        let writer = w.run();
        self.leader_connections
            .add_connection(
                self.leader_connections.main_leader_id().clone(),
                writer,
                read_kill_switch,
            )
            .unwrap();
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
            let kill_switch = r.run_with_id(self.tx.clone(), node.clone());
            let writer = w.run();
            self.leader_connections.insert(node.clone(), (kill_switch, writer));

            if auth_response.connected_to_leader {
                println!("Connected to leader: {}", node);
            }
        }
    }

    async fn determine_route(&mut self, command: &CommandToServer) -> Result<(), IoError> {
        match command.input.kind.clone() {
            | ClientAction::Get { key }
            | ClientAction::Ttl { key }
            | ClientAction::Incr { key }
            | ClientAction::Set { key, value: _ }
            | ClientAction::Append { key, value: _ }
            | ClientAction::SetWithExpiry { key, value: _, expiry: _ }
            | ClientAction::IndexGet { key, index: _ }
            | ClientAction::Decr { key } => self.route_command_by_key(command, key).await,
            | ClientAction::Delete { keys }
            | ClientAction::Exists { keys }
            | ClientAction::MGet { keys } => self.route_command_by_keys(command, keys).await,
            | _ => {
                let cmd = self.build_command_with_request_id(&command.command, &command.args);
                let Some(main_connection) =
                    self.leader_connections.get(self.leader_connections.main_leader_id())
                else {
                    return Err(IoError::Custom("Main connection not available".to_string()));
                };
                if let Err(e) =
                    main_connection.send(MsgToServer::Command(cmd.as_bytes().to_vec())).await
                {
                    println!("Failed to send command: {}", e);
                    return Err(IoError::Custom(format!("Failed to send command: {}", e)));
                }
                Ok(())
            },
        }
    }
}

pub enum BrokerMessage {
    FromServerWithId(PeerIdentifier, Result<QueryIO, IoError>),
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
