mod node_connections;
mod read_stream;
mod write_stream;
use crate::broker::node_connections::NodeConnections;
use crate::command::{CommandQueue, CommandToServer, InputContext, RoutingRule};
use duva::domains::cluster_actors::hash_ring::KeyOwnership;
use duva::domains::cluster_actors::replication::{ReplicationId, ReplicationRole};
use duva::domains::operation_logs::operation::LogEntry;
use duva::domains::{IoError, query_io::QueryIO};
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::sync::mpsc::Receiver;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::tokio::sync::oneshot;
use duva::prelude::uuid::Uuid;
use duva::prelude::{ELECTION_TIMEOUT_MAX, NodeReplInfo, Topology, anyhow};
use duva::prelude::{PeerIdentifier, tokio};
use duva::presentation::clients::request::{ClientAction, NonMutatingAction};
use duva::{
    domains::TSerdeReadWrite,
    prelude::{ConnectionRequest, ConnectionResponse},
};
use futures::future::try_join_all;

use node_connections::NodeConnection;
use read_stream::ServerStreamReader;
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

        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(2000);

        let seed_replid = auth_response.replication_id;
        let mut connections = NodeConnections::new(seed_replid.clone());

        connections.insert(
            seed_replid.clone(),
            NodeConnection {
                writer: w.run(),
                kill_switch: r.run(broker_tx.clone(), seed_replid.clone()),
                request_id: auth_response.request_id,
                peer_identifier: server_addr.clone(),
            },
        );

        Ok(Broker {
            tx: broker_tx,
            rx,
            client_id: Uuid::parse_str(&auth_response.client_id)?,
            topology: auth_response.topology,
            node_connections: connections,
        })
    }

    pub(crate) async fn run(mut self) {
        let mut queue = CommandQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                | BrokerMessage::FromServer(_, QueryIO::TopologyChange(topology)) => {
                    self.update_topology(topology).await;
                },

                | BrokerMessage::FromServer(repl_id, query_io) => {
                    let Some(context) = queue.pop() else {
                        continue;
                    };

                    if matches!(context.client_action, ClientAction::Mutating(..)) {
                        match self.node_connections.get_mut(&repl_id) {
                            | Some(connection) => connection.update_request_id(&query_io),
                            | None => {
                                println!("Connection not found after write operation");

                                // Log missing connection for debugging
                                tracing::warn!(
                                    replication_id = %repl_id,
                                    action = %format!("{:?}", context.client_action),
                                    "Connection not found after write operation"
                                );
                            },
                        }
                    }
                    context.finalize_or_requeue(&mut queue, query_io);
                },

                | BrokerMessage::FromServerError(repl_id, e) => match e {
                    | IoError::ConnectionAborted
                    | IoError::ConnectionReset
                    | IoError::ConnectionRefused
                    | IoError::NotConnected
                    | IoError::BrokenPipe => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            ELECTION_TIMEOUT_MAX,
                        ))
                        .await;
                        let removed_peer_id =
                            self.node_connections.remove_connection(&repl_id).await.unwrap();
                        self.discover_new_repl_leader(repl_id, removed_peer_id).await.unwrap();
                    },
                    | _ => {},
                },
                | BrokerMessage::ToServer(command) => {
                    if let Some(context) =
                        self.dispatch_command_to_server(command.routing_rule, command.context).await
                    {
                        queue.push(context);
                    };
                },
            }
        }
    }

    pub(crate) async fn authenticate(
        server_addr: &PeerIdentifier,
        auth_request: Option<ConnectionRequest>,
    ) -> Result<(ServerStreamReader, ServerStreamWriter, ConnectionResponse), IoError> {
        let mut stream =
            TcpStream::connect(server_addr.as_str()).await.map_err(|_| IoError::NotConnected)?;
        stream.serialized_write(auth_request.unwrap_or_default()).await?; // client_id not exist
        let auth_response: ConnectionResponse = stream.deserialized_read().await?;
        let (r, w) = stream.into_split();
        Ok((ServerStreamReader(r), ServerStreamWriter(w), auth_response))
    }

    // pull-based leader discovery
    // 1. if leader connection is lost, then discover new leader from follower candidates (TopologyChange not happened, LeaderConnections not updated)
    // 2. after removed leader from topology, and connection is lost, then discover new leader from follower candidates (TopologyChange happened, no new leader in topology)
    // 3. after leader added to topology, and connection is lost, then do nothing (TopologyChange happened, new leader in topology)
    async fn discover_new_repl_leader(
        &mut self,
        replication_id: ReplicationId,
        previous_leader: PeerIdentifier,
    ) -> anyhow::Result<()> {
        // ! ISSUE: replica set is queried and node connection is made
        // ! If no connection for the given replica set is not available, the user should not be able to make query, which leads to system unuvailability
        // ! We should make, therefore, some compromize that's based on some timing assumption - within this time, if connection is not established, we will abort the connection.
        // ! It means the following operation must be based on callback partern that's waiting for some node in the system notify the client of the event.
        let remaining_replicas: Vec<_> = self
            .topology
            .node_infos
            .iter()
            .filter(|n| n.peer_id != previous_leader && n.repl_id == replication_id)
            .map(|n| n.peer_id.clone())
            .collect();

        // TODO Potential improvement - idea could be where "multiplex" until anyone of them show positive for being a leader
        for follower in remaining_replicas {
            let _ = self.add_node_connection(follower).await;
        }

        // ! operation wise, seed node is just to not confuse user. If replacement is made, it'd be even more surprising to user because without user intervention,
        // ! system gives random result.
        Ok(())
    }

    async fn add_leader_conns_if_not_found(&mut self) {
        let nodes_to_add: Vec<_> = self
            .topology
            .node_infos
            .iter()
            .filter(|n| {
                n.repl_role == ReplicationRole::Leader
                    && !self.node_connections.contains_key(&n.repl_id)
            })
            .map(|n| n.peer_id.clone())
            .collect();

        for peer_id in nodes_to_add {
            let _ = self.add_node_connection(peer_id).await;
        }
    }

    async fn add_node_connection(&mut self, peer_id: PeerIdentifier) -> anyhow::Result<()> {
        let auth_req =
            ConnectionRequest { client_id: Some(self.client_id.to_string()), request_id: 0 };
        let Ok((server_stream_reader, server_stream_writer, auth_response)) =
            Self::authenticate(&peer_id, Some(auth_req)).await
        else {
            return Err(anyhow::anyhow!("Authentication failed!"));
        };

        if !auth_response.is_leader_node {
            return Err(anyhow::anyhow!("Only Leader connection is allowed!"));
        }

        self.node_connections.insert(
            auth_response.replication_id.clone(),
            NodeConnection {
                kill_switch: server_stream_reader
                    .run(self.tx.clone(), auth_response.replication_id),
                writer: server_stream_writer.run(),
                request_id: auth_response.request_id,
                peer_identifier: peer_id,
            },
        );
        Ok(())
    }

    async fn random_route(&self, client_action: ClientAction) -> anyhow::Result<usize> {
        self.node_connections.randomized_send(client_action).await?;
        Ok(1)
    }

    async fn route_info(&self, action: ClientAction) -> anyhow::Result<usize> {
        self.node_connections.send_to_seed(action).await?;
        Ok(1)
    }

    async fn dispatch_command_to_server(
        &self,
        routing_rule: RoutingRule,
        mut context: InputContext,
    ) -> Option<InputContext> {
        let action = context.client_action.clone();

        let res = match routing_rule {
            | RoutingRule::Any => self.random_route(action).await,
            | RoutingRule::Selective(entries) => {
                match self.topology.hash_ring.key_ownership(entries.iter().map(|e| e.key.as_str()))
                {
                    | Ok(node_mappings) => self.route_command_by_keys(action, node_mappings).await,
                    | Err(_not_found) => self.random_route(action).await,
                }
            },
            | RoutingRule::BroadCast => self.node_connections.send_all(action).await,
            | RoutingRule::Info => self.route_info(action).await,
        };

        let Ok(num_of_results) = res else {
            context
                .callback(QueryIO::Err("Failed to route command. Try again after ttl time".into()));
            return None;
        };
        context.set_expected_result_cnt(num_of_results);
        Some(context)
    }

    // The folowing operation is for both:
    // - single key operation
    // - multi key operaitons
    // When in comes to multi key operations, grouping logic needs to be considered
    async fn route_command_by_keys(
        &self,
        client_action: ClientAction,
        node_id_to_entries: KeyOwnership<'_>,
    ) -> anyhow::Result<usize> {
        let num_of_results = node_id_to_entries.len();
        try_join_all(node_id_to_entries.iter().map(|(node_id, routed_keys)| {
            let grouped_keys =
                routed_keys.iter().map(|key| key.to_string()).collect::<Vec<String>>();

            let new_action = match client_action {
                | ClientAction::NonMutating(NonMutatingAction::MGet { .. }) => {
                    NonMutatingAction::MGet { keys: grouped_keys }.into()
                },
                | ClientAction::NonMutating(NonMutatingAction::Exists { .. }) => {
                    NonMutatingAction::Exists { keys: grouped_keys }.into()
                },
                | ClientAction::Mutating(LogEntry::Delete { .. }) => {
                    LogEntry::Delete { keys: grouped_keys }.into()
                },
                | _ => client_action.clone(),
            };
            self.node_connections.send_to(node_id, new_action)
        }))
        .await?;
        Ok(num_of_results)
    }

    async fn update_topology(&mut self, topology: Topology) {
        //TODO topology version itself may need to be managed
        if self.topology.hash_ring.last_modified < topology.hash_ring.last_modified {
            self.topology = topology;
            self.add_leader_conns_if_not_found().await;

            self.node_connections
                .remove_outdated_connections(self.topology.hash_ring.get_replication_ids())
                .await;
        }
    }
}

pub enum BrokerMessage {
    FromServer(ReplicationId, QueryIO),
    FromServerError(ReplicationId, IoError),
    ToServer(CommandToServer),
}
impl BrokerMessage {
    pub fn from_input(
        action: ClientAction,
        callback: oneshot::Sender<(ClientAction, QueryIO)>,
    ) -> Self {
        let input_ctx = InputContext::new(action, callback);
        BrokerMessage::ToServer(CommandToServer {
            routing_rule: (&input_ctx.client_action).into(),
            context: input_ctx,
        })
    }
}
