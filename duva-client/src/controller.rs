use crate::broker::Broker;
use crate::broker::BrokerMessage;
use crate::cli_input::Input;
use duva::domains::IoError;
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
