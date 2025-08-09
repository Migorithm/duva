use crate::{
    domains::{
        cluster_actors::actor::ClusterCommandHandler,
        peers::{PeerMessage, peer::ListeningActorKillTrigger},
    },
    prelude::PeerIdentifier,
    types::Callback,
};
use tokio::select;

use super::{command::PeerCommand, connections::connection_types::ReadConnected};

#[derive(Debug)]
pub(crate) struct PeerListener {
    pub(crate) read_connected: ReadConnected,
    pub(crate) cluster_handler: ClusterCommandHandler,
    pub(crate) peer_id: PeerIdentifier,
}

impl PeerListener {
    pub(crate) fn spawn(
        read_connected: impl Into<ReadConnected>,
        cluster_handler: ClusterCommandHandler,
        peer_id: PeerIdentifier,
    ) -> ListeningActorKillTrigger {
        let mut listener = Self { read_connected: read_connected.into(), cluster_handler, peer_id };
        let (kill_trigger, kill_switch) = Callback::create();
        let handle = tokio::spawn({
            async move {
                select! {
                    _ = listener.start() => listener.read_connected.0,
                    // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
                    _ = kill_switch.recv() => listener.read_connected.0
                }
            }
        });

        ListeningActorKillTrigger::new(kill_trigger, handle)
    }

    async fn start(&mut self) {
        while let Ok(cmds) = self.read_command().await {
            for cmd in cmds {
                let _ = self
                    .cluster_handler
                    .send(PeerCommand { from: self.peer_id.clone(), msg: cmd })
                    .await;
            }
        }
    }

    async fn read_command(&mut self) -> anyhow::Result<Vec<PeerMessage>> {
        self.read_connected
            .read_values()
            .await?
            .into_iter()
            .map(PeerMessage::try_from)
            .collect::<Result<_, _>>()
    }
}
