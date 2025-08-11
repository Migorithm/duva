use duva::prelude::{
    anyhow::{self, Context},
    tokio::{self, io::AsyncWriteExt, net::tcp::OwnedWriteHalf},
};

pub struct ServerStreamWriter(pub(crate) OwnedWriteHalf);

impl ServerStreamWriter {
    pub async fn write_all(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        self.0.write_all(buf).await.context("Failed to send command")?;
        self.0.flush().await.context("Failed to flush stream")?;
        Ok(())
    }

    pub fn run(mut self) -> tokio::sync::mpsc::Sender<MsgToServer> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<MsgToServer>(2000);

        tokio::spawn(async move {
            while let Some(sendable) = rx.recv().await {
                match sendable {
                    | MsgToServer::Command(cmd) => {
                        if let Err(e) = self.write_all(&cmd).await {
                            println!("{e}");
                        };
                    },
                    | MsgToServer::Stop => break,
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
