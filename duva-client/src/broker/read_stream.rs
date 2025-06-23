use duva::{
    domains::IoError,
    domains::interface::TRead,
    prelude::tokio::{self, net::tcp::OwnedReadHalf, sync::oneshot},
};

use crate::broker::BrokerMessage;
use duva::prelude::PeerIdentifier;

pub struct ServerStreamReader(pub(crate) OwnedReadHalf);
impl ServerStreamReader {
    pub fn run_with_id(
        mut self,
        controller_sender: tokio::sync::mpsc::Sender<BrokerMessage>,
        peer_id: PeerIdentifier,
    ) -> oneshot::Sender<()> {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel::<()>();

        let future = async move {
            let controller_sender = controller_sender.clone();

            loop {
                match self.0.read_values().await {
                    | Ok(query_ios) => {
                        for query_io in query_ios {
                            let message =
                                BrokerMessage::FromServerWithId(peer_id.clone(), Ok(query_io));
                            if controller_sender.send(message).await.is_err() {
                                break;
                            }
                        }
                    },
                    | Err(IoError::ConnectionAborted) | Err(IoError::ConnectionReset) => {
                        let message = BrokerMessage::FromServerWithId(
                            peer_id.clone(),
                            Err(IoError::ConnectionAborted),
                        );
                        let _ = controller_sender.send(message).await;
                        println!("Connection reset or aborted for peer: {}", peer_id);
                        break;
                    },

                    | Err(e) => {
                        let message = BrokerMessage::FromServerWithId(peer_id.clone(), Err(e));
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
