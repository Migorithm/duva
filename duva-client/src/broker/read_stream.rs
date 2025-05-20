use duva::{
    domains::IoError,
    domains::interface::TRead,
    prelude::tokio::{self, net::tcp::OwnedReadHalf, sync::oneshot},
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
                    | Ok(query_ios) => {
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
                    | Err(IoError::ConnectionAborted) | Err(IoError::ConnectionReset) => {
                        let _ = controller_sender
                            .send(BrokerMessage::FromServer(Err(IoError::ConnectionAborted)))
                            .await;
                        println!("Connection reset or aborted");
                        break;
                    },

                    | Err(e) => {
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
