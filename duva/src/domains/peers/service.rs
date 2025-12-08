use crate::{
    domains::{
        TSerdeDynamicRead,
        cluster_actors::queue::ClusterActorSender,
        peers::{
            command::PeerMessage, connections::connection_types::ReadConnection,
            peer::ListeningActorKillTrigger,
        },
    },
    prelude::PeerIdentifier,
    types::Callback,
};
use tokio::{net::tcp::OwnedReadHalf, select};

use super::command::PeerCommand;

#[derive(Debug)]
pub(crate) struct PeerListener {
    pub(crate) read_connected: ReadConnection<OwnedReadHalf>,
    pub(crate) cluster_handler: ClusterActorSender,
    pub(crate) peer_id: PeerIdentifier,
}

impl PeerListener {
    pub(crate) fn spawn(
        read_connected: impl Into<ReadConnection<OwnedReadHalf>>,
        cluster_handler: ClusterActorSender,
        peer_id: PeerIdentifier,
    ) -> ListeningActorKillTrigger {
        let mut listener = Self { read_connected: read_connected.into(), cluster_handler, peer_id };
        let (kill_trigger, kill_switch) = Callback::create();
        let handle = tokio::spawn({
            async move {
                select! {
                    _ = listener.start() => listener.read_connected.stream ,
                    // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
                    _ = kill_switch.recv() => listener.read_connected.stream
                }
            }
        });

        ListeningActorKillTrigger::new(kill_trigger, handle)
    }

    async fn start(&mut self) {
        loop {
            if let Ok(msg) = self.read_command().await {
                let _ = self
                    .cluster_handler
                    .send(PeerCommand { from: self.peer_id.clone(), msg })
                    .await;
            }
        }
    }

    async fn read_command(&mut self) -> anyhow::Result<PeerMessage> {
        Ok(self.read_connected.receive_peer_msgs().await?)
    }
}
