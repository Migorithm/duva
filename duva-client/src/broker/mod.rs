mod input_queue;
mod node_connections;
mod read_stream;
mod write_stream;

use crate::command::{InputContext, build_command_with_request_id};

use crate::broker::node_connections::NodeConnections;
use crate::broker::write_stream::MsgToServer;
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
use node_connections::NodeConnection;
use read_stream::ServerStreamReader;
use write_stream::ServerStreamWriter;

pub struct Broker {
    pub(crate) tx: Sender<BrokerMessage>,
    pub(crate) rx: Receiver<BrokerMessage>,
    pub(crate) client_id: Uuid,
    pub(crate) topology: Topology,
    pub(crate) node_connections: NodeConnections,
    cluster_mode: bool,
}

impl Broker {
    pub(crate) async fn new(
        server_addr: &PeerIdentifier,
        cluster_mode: bool,
    ) -> anyhow::Result<Self> {
        let (r, w, auth_response) = Broker::authenticate(&server_addr.clone(), None).await?;

        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        let mut replication_id = None;
        if cluster_mode {
            let Some(repl_id) = auth_response.topology.hash_ring.get_replication_id(&server_addr)
            else {
                return Err(anyhow!(
                    "Cannot connect to replica: {}, try to connect to Leader",
                    server_addr
                ));
            };
            replication_id = Some(repl_id);
        };

        Ok(Broker {
            tx: broker_tx.clone(),
            rx,
            client_id: Uuid::parse_str(&auth_response.client_id)?,
            topology: auth_response.topology,
            node_connections: NodeConnections::new(
                server_addr.clone(),
                w.run(),
                r.run(broker_tx.clone(), server_addr.clone(), replication_id),
                auth_response.request_id,
            ),
            cluster_mode,
        })
    }

    pub(crate) async fn run(mut self) {
        let mut queue = InputQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                | BrokerMessage::FromServer(_, QueryIO::TopologyChange(topology)) => {
                    if !self.cluster_mode {
                        continue; // ignore topology changes in non-cluster mode
                    }
                    self.topology = topology;
                    self.update_leader_connections().await;
                },

                | BrokerMessage::FromServer(peer_id, query_io) => {
                    let Some(mut context) = queue.pop() else {
                        continue;
                    };

                    let connection = self
                        .node_connections
                        .get_mut(&peer_id)
                        .expect("Node connection should exist");

                    if let Some(index) =
                        Broker::extract_req_id(connection.request_id, &context.kind, &query_io)
                    {
                        connection.request_id = index;
                    }

                    context.append_result(query_io);

                    if !context.is_done() {
                        continue;
                    }

                    let result = context
                        .get_result()
                        .unwrap_or_else(|err| QueryIO::Err(err.to_string().into()));
                    context.callback.send((context.kind, result)).unwrap_or_else(|_| {
                        println!("Failed to send response to input callback");
                    });
                },

                | BrokerMessage::FromServerErrorFromCluster(repl_id, e) => match e {
                    | IoError::ConnectionAborted | IoError::ConnectionReset => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            LEADER_HEARTBEAT_INTERVAL_MAX,
                        ))
                        .await;
                        self.discover_new_leader(repl_id).await.unwrap();
                    },
                    | _ => {},
                },
                | BrokerMessage::FromServerErrorFromNode(peer_id, e) => match e {
                    | IoError::ConnectionAborted | IoError::ConnectionReset => {
                        self.node_connections.remove_connection(&peer_id).await;
                    },
                    | _ => {},
                },
                | BrokerMessage::ToServer(mut command) => {
                    if self.cluster_mode {
                        match self.determine_route(&command).await {
                            | Ok(num_of_results) => {
                                command.input_context.num_of_results = num_of_results;
                            },
                            | Err(e) => {
                                println!("Failed to determine route: {}", e);
                                continue;
                            },
                        }
                    } else {
                        // In non-cluster mode, we send commands directly to the server
                        if let Err(e) = self.default_route(command.command()).await {
                            println!("Failed to send command: {}", e);
                            continue;
                        }
                    }
                    queue.push(command.input_context);
                },
            }
        }
    }

    // ! CONSIDER IDEMPOTENCY RULE
    // !
    // ! If request is updating action yet receive error, we need to increase the request id
    // ! otherwise, server will not be able to process the next command
    fn extract_req_id(request_id: u64, kind: &ClientAction, query_io: &QueryIO) -> Option<u64> {
        if !kind.consensus_required() {
            return None;
        }
        match query_io {
            // * Current rule: s:value-idx:index_num
            | QueryIO::SimpleString(v) => {
                let s = String::from_utf8_lossy(v);
                IndexedValueCodec::decode_index(s).filter(|&id| id > request_id)
            },
            | QueryIO::Err(_) => Some(request_id + 1),
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
        self.node_connections.remove_connection(peer_id).await;

        // remove connections to nodes that are not in the topology anymore
        for node in self.topology.node_infos.clone() {
            if node.repl_id != replication_id {
                continue; // skip nodes with different replication id
            }
            if self.node_connections.contains_key(&node.peer_id) {
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
            if self.node_connections.contains_key(&node.peer_id) {
                continue; // skip already connected nodes
            }
            let _ = self.add_leader_connection(node).await;
        }
        self.node_connections
            .remove_outdated_connections(
                self.topology.node_infos.iter().map(|n| n.peer_id.clone()).collect(),
            )
            .await;
    }
    async fn add_leader_connection(&mut self, node: NodeReplInfo) -> Option<()> {
        let auth_req = AuthRequest { client_id: Some(self.client_id.to_string()), request_id: 0 };
        let Ok((r, w, auth_response)) = Self::authenticate(&node.peer_id, Some(auth_req)).await
        else {
            return None;
        };
        if !auth_response.connected_to_leader {
            return None;
        }
        let kill_switch = r.run(self.tx.clone(), node.peer_id.clone(), Some(node.repl_id));
        let writer = w.run();

        self.node_connections.insert(
            node.peer_id,
            NodeConnection::new(writer, kill_switch, auth_response.request_id),
        );
        self.topology = auth_response.topology;

        Some(())
    }

    async fn determine_route(&mut self, command: &CommandToServer) -> Result<usize, IoError> {
        match command.input_context.kind.clone() {
            | ClientAction::Get { key }
            | ClientAction::Ttl { key }
            | ClientAction::Incr { key }
            | ClientAction::Set { key, value: _ }
            | ClientAction::Append { key, value: _ }
            | ClientAction::SetWithExpiry { key, value: _, expiry: _ }
            | ClientAction::IndexGet { key, index: _ }
            | ClientAction::Decr { key } => self.route_command_by_key(command.command(), key).await,
            | ClientAction::Delete { keys }
            | ClientAction::Exists { keys }
            | ClientAction::MGet { keys } => {
                self.route_command_by_keys(command.command(), keys).await
            },
            | ClientAction::Keys { pattern: _ } => self.route_command_to_all(&command).await,
            | _ => self.default_route(command.command()).await,
        }
    }
    async fn route_command_by_keys(
        &mut self,
        command: (String, Vec<String>),
        keys: Vec<String>,
    ) -> Result<usize, IoError> {
        let mut node_id_to_keys = std::collections::HashMap::new();
        for key in keys {
            let node_id = match self.topology.hash_ring.get_node_id_for_key(key.as_ref()) {
                | Some(id) => id,
                | None => {
                    continue;
                },
            };
            node_id_to_keys.entry(node_id).or_insert_with(Vec::new).push(key);
        }

        let num_of_results = node_id_to_keys.len();
        for (node_id, keys) in node_id_to_keys.into_iter() {
            let command = command.0.clone();
            self.send_command((command, keys), node_id).await?;
        }

        Ok(num_of_results)
    }
    async fn default_route(&mut self, command: (String, Vec<String>)) -> Result<usize, IoError> {
        let node_id = self
            .node_connections
            .get_first_node_id()
            .map_err(|e| IoError::Custom(format!("Failed to get first node id: {}", e)))?;
        self.send_command(command, node_id).await?;
        Ok(1)
    }

    async fn route_command_by_key(
        &self,
        command: (String, Vec<String>),
        key: String,
    ) -> Result<usize, IoError> {
        let Some(node_id) = self.topology.hash_ring.get_node_id_for_key(key.as_ref()) else {
            return Err(IoError::Custom(format!("Failed to get node id from key {}", key)));
        };

        self.send_command(command, node_id).await?;
        Ok(1)
    }

    async fn route_command_to_all(&self, command: &CommandToServer) -> Result<usize, IoError> {
        for node_id in self.topology.node_infos.iter().map(|n| n.peer_id.clone()) {
            self.send_command(command.command(), &node_id).await?;
        }
        Ok(self.topology.node_infos.len())
    }

    async fn send_command(
        &self,
        command: (String, Vec<String>),
        node_id: &PeerIdentifier,
    ) -> Result<(), IoError> {
        let connection = self.node_connections.get(node_id).or_else(|_| {
            Err(IoError::Custom(format!("No connection found for node id: {}", node_id)))
        })?;
        let cmd = build_command_with_request_id(&command.0, connection.request_id, &command.1);

        let _ = connection
            .send(MsgToServer::Command(cmd.as_bytes().to_vec()))
            .await
            .map_err(|e| IoError::Custom(format!("Failed to send command: {}", e)))?;
        Ok(())
    }
}

pub enum BrokerMessage {
    FromServer(PeerIdentifier, QueryIO),
    FromServerErrorFromCluster(ReplicationId, IoError),
    FromServerErrorFromNode(PeerIdentifier, IoError),
    ToServer(CommandToServer),
}
impl BrokerMessage {
    pub fn from_command(command: String, args: Vec<&str>, input_context: InputContext) -> Self {
        BrokerMessage::ToServer(CommandToServer {
            command,
            args: args.iter().map(|s| s.to_string()).collect(),
            input_context,
        })
    }
}

pub struct CommandToServer {
    pub command: String,
    pub args: Vec<String>,
    pub input_context: InputContext,
}

impl CommandToServer {
    pub fn command(&self) -> (String, Vec<String>) {
        (self.command.clone(), self.args.clone())
    }
}
