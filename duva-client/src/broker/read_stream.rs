use crate::broker::BrokerMessage;
use duva::domains::TSerdeRead;

use duva::prelude::tokio::{self, net::tcp::OwnedReadHalf, sync::oneshot};
use duva::prelude::{BytesMut, ReplicationId};

pub struct ServerStreamReader(pub(crate) OwnedReadHalf);
impl ServerStreamReader {
    pub fn run(
        mut self,
        controller_sender: tokio::sync::mpsc::Sender<BrokerMessage>,
        replication_id: ReplicationId,
    ) -> oneshot::Sender<()> {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();

        let future = async move {
            let controller_sender = controller_sender.clone();
            loop {
                let mut buffer = BytesMut::new();
                match self.0.deserialized_reads(&mut buffer).await {
                    Ok(server_responses) => {
                        for res in server_responses {
                            if controller_sender
                                .send(BrokerMessage::FromServer(replication_id.clone(), res))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        let message = BrokerMessage::FromServerError(replication_id.clone(), e);
                        if controller_sender.send(message).await.is_err() {
                            break;
                        }

                        // ! without this, test_removed_connection and test_discover_leader fail. Why?
                        break;
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
