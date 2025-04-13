use crate::broker::Broker;
use crate::broker::BrokerMessage;
use crate::cli_input::Input;
use crate::command::ClientInputKind;
use duva::domains::IoError;
use duva::domains::query_parsers::query_io::QueryIO;
use duva::prelude::tokio;
use duva::prelude::tokio::io::AsyncWriteExt;
use duva::prelude::tokio::net::tcp::OwnedReadHalf;
use duva::prelude::tokio::net::tcp::OwnedWriteHalf;
use duva::prelude::tokio::sync::mpsc::Sender;
use duva::prelude::tokio::sync::oneshot;
use duva::prelude::uuid::Uuid;
use duva::services::interface::TRead;

// TODO Read actor and Write actor
pub struct ClientController<T> {
    pub broker_tx: Sender<BrokerMessage>,
    pub target: T,
}

impl<T> ClientController<T> {
    pub async fn new(editor: T, server_addr: &str) -> Self {
        let (r, w, mut auth_response) = Broker::authenticate(server_addr, None).await.unwrap();

        auth_response.cluster_nodes.push(server_addr.to_string().into());
        let (broker_tx, rx) = tokio::sync::mpsc::channel::<BrokerMessage>(100);

        let broker = Broker {
            tx: broker_tx.clone(),
            rx,
            to_server: w.run(),
            client_id: Uuid::parse_str(&auth_response.client_id).unwrap(),
            request_id: auth_response.request_id,
            latest_known_index: 0,
            cluster_nodes: auth_response.cluster_nodes,
            read_kill_switch: Some(r.run(broker_tx.clone())),
        };
        tokio::spawn(broker.run());
        Self { broker_tx, target: editor }
    }

    pub fn print_res(&self, kind: ClientInputKind, query_io: QueryIO) {
        use ClientInputKind::*;
        match kind {
            Ping | Get | IndexGet | Echo | Config | Save | Info | ClusterForget | Role
            | ReplicaOf | ClusterInfo => match query_io {
                QueryIO::Null => println!("(nil)"),
                QueryIO::SimpleString(value) => println!("{value}"),
                QueryIO::BulkString(value) => println!("{value}"),
                QueryIO::Err(value) => {
                    println!("(error) {value}");
                },
                _err => {
                    println!("Unexpected response format");
                },
            },
            Del | Exists => {
                let QueryIO::SimpleString(value) = query_io else {
                    println!("Unexpected response format");
                    return;
                };
                let deleted_count = value.parse::<u64>().unwrap();
                println!("(integer) {}", deleted_count);
            },
            Set => {
                match query_io {
                    QueryIO::SimpleString(_) => {
                        println!("OK");
                        return;
                    },
                    QueryIO::Err(value) => {
                        println!("(error) {value}");
                        return;
                    },
                    _ => {
                        println!("Unexpected response format");
                        return;
                    },
                };
            },
            Keys => {
                let QueryIO::Array(value) = query_io else {
                    println!("Unexpected response format");
                    return;
                };
                for (i, item) in value.into_iter().enumerate() {
                    let QueryIO::BulkString(value) = item else {
                        println!("Unexpected response format");
                        break;
                    };
                    println!("{i}) \"{value}\"");
                }
            },
            ClusterNodes => {
                let QueryIO::Array(value) = query_io else {
                    println!("Unexpected response format");
                    return;
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
    }
}

pub struct ServerStreamReader(pub(crate) OwnedReadHalf);
impl ServerStreamReader {
    pub fn run(
        mut self,
        controller_sender: tokio::sync::mpsc::Sender<BrokerMessage>,
    ) -> oneshot::Sender<()> {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel::<()>();

        let future = async move {
            let controller_sender = controller_sender.clone();

            loop {
                match self.0.read_values().await {
                    Ok(query_ios) => {
                        for query_io in query_ios {
                            if controller_sender
                                .send(BrokerMessage::FromServer(Ok(query_io)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    },
                    Err(IoError::ConnectionAborted) | Err(IoError::ConnectionReset) => {
                        let _ = controller_sender
                            .send(BrokerMessage::FromServer(Err(IoError::ConnectionAborted)))
                            .await;
                        println!("Connection reset or aborted");
                        break;
                    },

                    Err(e) => {
                        if controller_sender.send(BrokerMessage::FromServer(Err(e))).await.is_err()
                        {
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

pub struct CommandToServer {
    pub command: String,
    pub args: Vec<String>,
    pub input: Input,
}

pub struct ServerStreamWriter(pub(crate) OwnedWriteHalf);

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

    pub fn run(mut self) -> tokio::sync::mpsc::Sender<MsgToServer> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<MsgToServer>(100);

        tokio::spawn(async move {
            while let Some(sendable) = rx.recv().await {
                match sendable {
                    MsgToServer::Command(cmd) => {
                        if let Err(e) = self.write_all(&cmd).await {
                            println!("{e}");
                        };
                    },
                    MsgToServer::Stop => break,
                }
            }
        });
        tx
    }
}

pub enum MsgToServer {
    Command(Vec<u8>),
    Stop,
}
