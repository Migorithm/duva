mod input_queue;
mod node_connections;
mod read_stream;
mod write_stream;

use crate::broker::node_connections::NodeConnections;
use crate::broker::write_stream::MsgToServer;
use crate::command::{
    CommandEntry, CommandToServer, Input, InputContext, RoutingRule, build_command_with_request_id,
};
use collections::HashMap;
use duva::domains::caches::cache_manager::IndexedValueCodec;
use duva::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use duva::domains::query_io::QueryIO::SessionRequest;
use duva::domains::{IoError, query_io::QueryIO};
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
use futures::FutureExt;
use futures::future::try_join_all;
use input_queue::InputQueue;
use node_connections::NodeConnection;
use read_stream::ServerStreamReader;
use std::collections;
use write_stream::ServerStreamWriter;

pub struct Broker {
    pub(crate) tx: Sender<BrokerMessage>,
    pub(crate) rx: Receiver<BrokerMessage>,
    pub(crate) client_id: Uuid,
    pub(crate) topology: Topology,
    pub(crate) node_connections: NodeConnections,
}

impl Broker {
    pub(crate) async fn new(server_addr: &PeerIdentifier) -> anyhow::Result<Self> {
        let (r, w, auth_response) = Broker::authenticate(&server_addr.clone(), None).await?;

        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        let replication_id = auth_response.replication_id;
        let mut broker = Broker {
            tx: broker_tx.clone(),
            rx,
            client_id: Uuid::parse_str(&auth_response.client_id)?,
            topology: auth_response.topology,
            node_connections: NodeConnections::new(
                replication_id.clone(),
                w.run(),
                r.run(broker_tx.clone(), replication_id),
                auth_response.request_id,
            ),
        };
        broker.update_leader_connections().await;
        Ok(broker)
    }

    pub(crate) async fn run(mut self) {
        let mut queue = InputQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                | BrokerMessage::FromServer(_, QueryIO::TopologyChange(topology)) => {
                    self.topology = topology;
                    self.update_leader_connections().await;
                },

                | BrokerMessage::FromServer(repl_id, query_io) => {
                    let Some(mut context) = queue.pop() else {
                        continue;
                    };

                    let connection = self
                        .node_connections
                        .get_mut(&repl_id)
                        .expect("Node connection should exist");

                    if let Some(index) =
                        Broker::extract_req_id(connection.request_id, &context.kind, &query_io)
                    {
                        connection.request_id = index;
                    }

                    context.append_result(query_io);

                    if !context.is_done() {
                        queue.push(context);
                        continue;
                    }

                    let result = context
                        .get_result()
                        .unwrap_or_else(|err| QueryIO::Err(err.to_string().into()));
                    context.callback.send((context.kind, result)).unwrap_or_else(|_| {
                        println!("Failed to send response to input callback");
                    });
                },

                | BrokerMessage::FromServerError(repl_id, e) => match e {
                    | IoError::ConnectionAborted
                    | IoError::ConnectionReset
                    | IoError::ConnectionRefused
                    | IoError::NotConnected
                    | IoError::BrokenPipe => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            LEADER_HEARTBEAT_INTERVAL_MAX,
                        ))
                        .await;
                        self.discover_new_leader(repl_id).await.unwrap();
                    },
                    | _ => {},
                },
                | BrokerMessage::ToServer(mut command) => {
                    let mut context = command.input_context;
                    let action = context.kind.clone();
                    let res = match command.routing_rule {
                        | RoutingRule::Any => self.default_route(action).await,
                        | RoutingRule::Selective(entries) => {
                            self.route_command_by_keys(action, entries).await
                        },
                        | RoutingRule::BroadCast => self.route_command_to_all(action).await,
                    };
                    match res {
                        | Ok(num_of_results) => {
                            context.set_expected_result_cnt(num_of_results);
                        },
                        | Err(_e) => {
                            continue;
                        },
                    }
                    queue.push(context);
                },
            }
        }
    }

    // ! CONSIDER IDEMPOTENCY RULE
    // !
    // ! If request is updating action yet receive error, we need to increase the request id
    // ! otherwise, server will not be able to process the next command
    fn extract_req_id(request_id: u64, kind: &ClientAction, query_io: &QueryIO) -> Option<u64> {
        kind.to_write_request()?;
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
        self.node_connections.remove_connection(&replication_id.clone()).await;

        // remove connections to nodes that are not in the topology anymore
        for node in self.topology.node_infos.clone() {
            if node.repl_id != replication_id {
                continue; // skip nodes with different replication id
            }
            if let Some(()) = self.add_leader_connection(node.clone()).await {
                return Ok(());
            }
        }
        if self.node_connections.is_empty() {
            return Err(IoError::Custom("No connections available for replication id".to_string()));
        }
        Ok(())
    }

    async fn update_leader_connections(&mut self) {
        for node in self.topology.node_infos.clone() {
            if ReplicationRole::Leader != node.repl_role.clone() {
                continue; // skip non-leader nodes
            }
            if self.node_connections.contains_key(&node.repl_id) {
                continue; // skip already connected nodes
            }
            let _ = self.add_leader_connection(node).await;
        }
        self.node_connections
            .remove_outdated_connections(self.topology.hash_ring.get_replication_ids())
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
        let kill_switch = r.run(self.tx.clone(), auth_response.replication_id.clone());
        let writer = w.run();

        self.node_connections.insert(
            auth_response.replication_id,
            NodeConnection::new(writer, kill_switch, auth_response.request_id),
        );
        Some(())
    }

    // 'default_route' is used when given request does NOT have to be sent to a specific node
    async fn default_route(&self, client_action: ClientAction) -> Result<usize, IoError> {
        self.node_connections.send_to(None, client_action).await?;
        Ok(1)
    }

    async fn route_command_by_keys(
        &mut self,
        client_action: ClientAction,
        entries: Vec<CommandEntry>,
    ) -> Result<usize, IoError> {
        let Ok(mut node_id_to_entries) = self
            .topology
            .hash_ring
            .list_replids_for_keys(&entries.iter().map(|e| e.key.as_str()).collect::<Vec<&str>>())
        else {
            // If routing failed, we fall back to default routing
            return self.default_route(client_action).await;
        };

        let num_of_results = node_id_to_entries.len();

        try_join_all(node_id_to_entries.iter().map(|(node_id, routed_keys)| {
            let keys = routed_keys.iter().map(|key| key.to_string()).collect::<Vec<String>>();
            let new_action = match client_action {
                | ClientAction::MGet { .. } => ClientAction::MGet { keys },
                | ClientAction::Exists { .. } => ClientAction::Exists { keys },
                | ClientAction::Delete { .. } => ClientAction::Delete { keys },
                | _ => client_action.clone(),
            };
            self.node_connections.send_to(Some(node_id), new_action)
        }))
        .await?;

        Ok(num_of_results)
    }

    async fn route_command_to_all(&self, client_action: ClientAction) -> Result<usize, IoError> {
        try_join_all(
            self.node_connections
                .keys()
                .map(|node_id| self.node_connections.send_to(Some(node_id), client_action.clone())),
        )
        .await?;
        Ok(self.node_connections.len())
    }
}

pub enum BrokerMessage {
    FromServer(ReplicationId, QueryIO),
    FromServerError(ReplicationId, IoError),
    ToServer(CommandToServer),
}
impl BrokerMessage {
    pub fn from_input(input: Input, input_context: InputContext) -> Self {
        BrokerMessage::ToServer(CommandToServer {
            routing_rule: (&input_context.kind).into(),
            input,
            input_context,
        })
    }
}
