use duva::{
    domains::IoError,
    prelude::tokio::{
        self,
        io::AsyncWriteExt,
        net::tcp::{OwnedReadHalf, OwnedWriteHalf},
        sync::oneshot,
    },
    services::interface::TRead,
};

use crate::broker::BrokerMessage;

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
