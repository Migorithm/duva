use duva::{
    domains::IoError,
    domains::interface::TRead,
    prelude::tokio::{self, net::tcp::OwnedReadHalf, sync::oneshot},
};
use duva::domains::cluster_actors::replication::ReplicationId;
use crate::broker::{BrokerMessage};

pub struct ServerStreamReader(pub(crate) OwnedReadHalf);
impl ServerStreamReader {
    pub fn run(
        mut self,
        controller_sender: tokio::sync::mpsc::Sender<BrokerMessage>,
        replication_id: ReplicationId
    ) -> oneshot::Sender<()> {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel::<()>();

        let future = async move {
            let controller_sender = controller_sender.clone();

            loop {
                match self.0.read_values().await {
                    | Ok(query_ios) => {
                        for query_io in query_ios {
                            let message =
                                BrokerMessage::FromServer(query_io);
                            if controller_sender.send(message).await.is_err() {
                                break;
                            }
                        }
                    },
                    | Err(IoError::ConnectionAborted) | Err(IoError::ConnectionReset) => {
                        let message = BrokerMessage::FromServerError(
                            replication_id.clone(),
                            IoError::ConnectionAborted,
                        );
                        let _ = controller_sender.send(message).await;
                        break;
                    },

                    | Err(e) => {
                        let message = BrokerMessage::FromServerError(replication_id.clone(), e);
                        if controller_sender.send(message).await.is_err() {
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
