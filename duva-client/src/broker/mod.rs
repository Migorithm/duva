mod input_queue;
mod leader_connections;
mod read_stream;
mod write_stream;

use crate::command::InputKind;

use crate::broker::leader_connections::LeaderConnections;
use duva::domains::caches::cache_manager::IndexedValueCodec;
use duva::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use duva::domains::{IoError, query_io::QueryIO};
use duva::prelude::anyhow::anyhow;
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::sync::mpsc::Receiver;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::uuid::Uuid;
use duva::prelude::{LEADER_HEARTBEAT_INTERVAL_MAX, NodeReplInfo, Topology, anyhow};
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
    pub(crate) client_id: Uuid,
    pub(crate) request_id: u64,
    pub(crate) topology: Topology,
    pub(crate) leader_connections: LeaderConnections,
}

impl Broker {
    pub(crate) async fn new(server_addr: &PeerIdentifier) -> anyhow::Result<Self> {
        let (r, w, auth_response) = Broker::authenticate(&server_addr.clone(), None).await?;

        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        let Some(replication_id) =
            auth_response.topology.hash_ring.get_replication_id(&server_addr)
        else {
            return Err(anyhow!(
                "Cannot connect to replica: {}, try to connect to Leader",
                server_addr
            ));
        };

        Ok(Broker {
            tx: broker_tx.clone(),
            rx,
            client_id: Uuid::parse_str(&auth_response.client_id)?,
            request_id: auth_response.request_id,
            topology: auth_response.topology,
            leader_connections: LeaderConnections::new(
                server_addr.clone(),
                w.run(),
                r.run(broker_tx.clone(), replication_id),
            ),
        })
    }
    pub(crate) async fn run(mut self) {
        let mut queue = InputQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                | BrokerMessage::FromServer(QueryIO::TopologyChange(topology)) => {
                    self.topology = topology;
                    self.update_leader_connections().await;
                },

                | BrokerMessage::FromServer(query_io) => {
                    let Some(input) = queue.pop() else {
                        continue;
                    };

                    match input {
                        | InputKind::SingleNodeInput { kind, callback } => {
                            if let Some(index) = self.extract_req_id(&kind, &query_io) {
                                self.request_id = index;
                            };

                            callback.send((kind, query_io)).unwrap_or_else(|_| {
                                println!("Failed to send response to input callback");
                            });
                        },
                        | InputKind::MultipleNodesInput { kind, callback, mut results } => {
                            results.push(query_io);
                            if results.len() != self.leader_connections.len() {
                                let input =
                                    InputKind::MultipleNodesInput { kind, callback, results };
                                queue.push(input);
                            } else {
                                let queries = results
                                    .into_iter()
                                    .reduce(|a, b| {
                                        a.merge(b).expect("Either one of them should be array")
                                    })
                                    .unwrap();
                                callback.send((kind, queries)).unwrap_or_else(|_| {
                                    println!("Failed to send response to input callback");
                                });
                            }
                        },
                    }
                },

                | BrokerMessage::FromServerError(repl_id, e) => match e {
                    | IoError::ConnectionAborted | IoError::ConnectionReset => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            LEADER_HEARTBEAT_INTERVAL_MAX,
                        ))
                        .await;
                        self.discover_new_leader(repl_id).await.unwrap();
                    },
                    | _ => {},
                },
                | BrokerMessage::ToServer(command) => {
                    if let Err(err) = self.determine_route(&command).await {
                        println!("{:?}", err);
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

    // pull-based leader discovery
    // 1. if leader connection is lost, then discover new leader from follower candidates (TopologyChange not happened, LeaderConnections not updated)
    // 2. after removed leader from topology, and connection is lost, then discover new leader from follower candidates (TopologyChange happened, no new leader in topology)
    // 3. after leader added to topology, and connection is lost, then do nothing (TopologyChange happened, new leader in topology)
    async fn discover_new_leader(&mut self, replication_id: ReplicationId) -> Result<(), IoError> {
        // remove disconnected leader connection
        let Some(peer_id) = self.topology.hash_ring.get_node_id(&replication_id) else {
            return Err(IoError::Custom(format!(
                "Failed to get peer id for replication id: {}",
                replication_id
            )));
        };
        self.leader_connections.remove_connection(peer_id).await;

        // remove connections to nodes that are not in the topology anymore
        for node in self.topology.node_infos.clone() {
            if node.repl_id != replication_id.to_string() {
                continue; // skip nodes with different replication id
            }
            if self.leader_connections.contains_key(&PeerIdentifier(node.peer_id.clone())) {
                continue; // skip already connected leader nodes, covers 1, 3
            }
            if let Some(()) = self.add_leader_connection(node).await {
                return Ok(());
            }
        }
        Err(IoError::Custom(format!(
            "Failed to discover new leader for replication id: {}",
            replication_id
        )))
    }

    async fn update_leader_connections(&mut self) {
        for node in self.topology.node_infos.clone() {
            if ReplicationRole::Leader != node.repl_role.clone().into() {
                continue; // skip non-leader nodes
            }
            if self.leader_connections.contains_key(&PeerIdentifier(node.peer_id.clone())) {
                continue; // skip already connected nodes
            }
            let _ = self.add_leader_connection(node).await;
        }
        self.leader_connections
            .remove_outdated_connections(
                self.topology
                    .node_infos
                    .iter()
                    .map(|n| PeerIdentifier(n.peer_id.clone()))
                    .collect(),
            )
            .await;
    }
    async fn add_leader_connection(&mut self, node: NodeReplInfo) -> Option<()> {
        let auth_req = AuthRequest {
            client_id: Some(self.client_id.to_string()),
            request_id: self.request_id,
        };
        let Ok((r, w, auth_response)) =
            Self::authenticate(&PeerIdentifier(node.peer_id.clone()), Some(auth_req)).await
        else {
            return None;
        };
        if !(auth_response.connected_to_leader) {
            return None;
        }
        let kill_switch = r.run(self.tx.clone(), node.repl_id.clone().into());
        let writer = w.run();
        self.leader_connections.insert(PeerIdentifier(node.peer_id.clone()), (kill_switch, writer));
        self.topology = auth_response.topology;

        Some(())
    }

    async fn determine_route(&mut self, command: &CommandToServer) -> Result<(), IoError> {
        match command.input.kind().clone() {
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
            | ClientAction::Keys { pattern: _ } => self.route_command_to_all(&command).await,
            | _ => self.default_route(&command).await,
        }
    }
    async fn default_route(&mut self, command: &&CommandToServer) -> Result<(), IoError> {
        let cmd = self.build_command_with_request_id(&command.command, &command.args);
        let Some(connection) = self.leader_connections.first() else {
            return Err(IoError::Custom("No connections available".to_string()));
        };
        connection
            .send(MsgToServer::Command(cmd.as_bytes().to_vec()))
            .await
            .map_err(|e| IoError::Custom(format!("Failed to send command: {}", e)))
    }
    async fn route_command_to_all(&mut self, command: &&CommandToServer) -> Result<(), IoError> {
        let cmd = self.build_command_with_request_id(&command.command, &command.args);
        for (peer_id, connection) in self.leader_connections.entries() {
            if let Err(e) = connection.send(MsgToServer::Command(cmd.as_bytes().to_vec())).await {
                return Err(IoError::Custom(format!(
                    "Failed to send command to {}: {}",
                    peer_id, e
                )));
            }
        }
        Ok(())
    }
    async fn route_command_by_keys(
        &self,
        command: &CommandToServer,
        keys: Vec<String>,
    ) -> Result<(), IoError> {
        let Some(node_id) = self
            .topology
            .hash_ring
            .get_node_id_for_keys(&keys.iter().map(|key| key.as_str()).collect::<Vec<&str>>())
        else {
            return Err(IoError::Custom(format!("Failed to get node ids from keys {:?}", keys)));
        };

        if let Some(value) = self.send_command(command, node_id).await {
            return value;
        }
        Ok(())
    }

    async fn route_command_by_key(
        &self,
        command: &CommandToServer,
        key: String,
    ) -> Result<(), IoError> {
        let Some(node_id) = self.topology.hash_ring.get_node_id_for_key(key.as_ref()) else {
            return Err(IoError::Custom(format!("Failed to get node id from key {}", key)));
        };

        if let Some(value) = self.send_command(command, node_id).await {
            return value;
        }
        Ok(())
    }

    async fn send_command(
        &self,
        command: &CommandToServer,
        node_id: &PeerIdentifier,
    ) -> Option<Result<(), IoError>> {
        let Some(connection) = self.leader_connections.get(node_id) else {
            return Some(Err(IoError::Custom(format!(
                "Failed to get connections for node id {:?}",
                node_id
            ))));
        };
        let cmd = self.build_command_with_request_id(&command.command, &command.args);
        if let Err(e) = connection.send(MsgToServer::Command(cmd.as_bytes().to_vec())).await {
            return Some(Err(IoError::Custom(format!("Failed to send command: {}", e))));
        }
        None
    }
}

pub enum BrokerMessage {
    FromServer(QueryIO),
    FromServerError(ReplicationId, IoError),
    ToServer(CommandToServer),
}
impl BrokerMessage {
    pub fn from_command(command: String, args: Vec<&str>, input: InputKind) -> Self {
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
    pub input: InputKind,
}
