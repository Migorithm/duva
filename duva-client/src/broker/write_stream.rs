use duva::prelude::tokio::{self, io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

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
