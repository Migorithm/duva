mod input_queue;
mod read_stream;
mod write_stream;

use crate::command::Input;
use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;
use duva::domains::{IoError, query_parsers::query_io::QueryIO};
use duva::prelude::PeerIdentifier;
use duva::prelude::tokio;
use duva::prelude::tokio::net::TcpStream;
use duva::prelude::tokio::sync::mpsc::Receiver;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::uuid::Uuid;
use duva::presentation::clients::request::ClientAction;
use duva::{
    clients::authentications::{AuthRequest, AuthResponse},
    services::interface::TSerdeReadWrite,
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
    pub(crate) latest_known_index: u64,
    pub(crate) cluster_nodes: Vec<PeerIdentifier>,
    pub(crate) read_kill_switch: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Broker {
    pub(crate) async fn run(mut self) {
        let mut queue = InputQueue::default();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                BrokerMessage::FromServer(Ok(QueryIO::TopologyChange(topology))) => {
                    self.cluster_nodes = topology;
                },

                BrokerMessage::FromServer(Ok(query_io)) => {
                    let Some(input) = queue.pop() else {
                        continue;
                    };

                    if let Some(index) = self.need_index_increase(&input.kind, &query_io) {
                        if index > self.latest_known_index {
                            self.latest_known_index = index;
                        }
                    }
                    self.may_update_request_id(&input.kind);

                    input.callback.send((input.kind, query_io)).unwrap_or_else(|_| {
                        println!("Failed to send response to input callback");
                    });
                },
                BrokerMessage::FromServer(Err(e)) => match e {
                    IoError::ConnectionAborted | IoError::ConnectionReset => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            LEADER_HEARTBEAT_INTERVAL_MAX,
                        ))
                        .await;
                        self.discover_leader().await.unwrap();
                    },
                    _ => {},
                },
                BrokerMessage::ToServer(command) => {
                    let cmd = self.build_command(&command.command, command.args);
                    if let Err(e) =
                        self.to_server.send(MsgToServer::Command(cmd.as_bytes().to_vec())).await
                    {
                        println!("Failed to send command: {}", e);
                    }
                    queue.push(command.input);
                },
            }
        }
    }

    pub fn build_command(&self, cmd: &str, args: Vec<String>) -> String {
        // Build the valid RESP command
        let mut command =
            format!("!{}\r\n*{}\r\n${}\r\n{}\r\n", self.request_id, args.len() + 1, cmd.len(), cmd);
        for arg in args {
            command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        command
    }

    fn need_index_increase(&mut self, kind: &ClientAction, query_io: &QueryIO) -> Option<u64> {
        if matches!(kind, ClientAction::Set { .. } | ClientAction::Delete { .. }) {
            if let QueryIO::SimpleString(v) = query_io {
                let rindex = v.split_whitespace().last().unwrap();
                return rindex.parse::<u64>().ok();
            }
        }
        None
    }

    fn may_update_request_id(&mut self, input: &ClientAction) {
        match input {
            ClientAction::Set { .. }
            | ClientAction::Delete { .. }
            | ClientAction::Save
            | ClientAction::Incr { .. }
            | ClientAction::Decr { .. } => {
                self.request_id += 1;
            },
            _ => {},
        }
    }

    pub(crate) async fn authenticate(
        server_addr: &str,
        auth_request: Option<AuthRequest>,
    ) -> Result<(ServerStreamReader, ServerStreamWriter, AuthResponse), IoError> {
        let mut stream = TcpStream::connect(server_addr).await.unwrap();

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
        for node in &self.cluster_nodes {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            println!("Trying to connect to node: {}...", node);

            let auth_req = AuthRequest {
                client_id: Some(self.client_id.to_string()),
                request_id: self.request_id,
            };
            let Ok((r, w, auth_response)) = Self::authenticate(node, Some(auth_req)).await else {
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
