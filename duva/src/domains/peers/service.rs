use crate::{
    domains::{
        cluster_actors::actor::ClusterCommandHandler,
        interface::TRead,
        peers::{PeerMessage, peer::ListeningActorKillTrigger},
    },
    prelude::PeerIdentifier,
};
use tokio::{net::tcp::OwnedReadHalf, select};

use super::{command::PeerCommand, connections::connected_types::ReadConnected};

#[cfg(test)]
static ATOMIC: std::sync::atomic::AtomicI16 = std::sync::atomic::AtomicI16::new(0);

pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

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
        let listener = Self { read_connected: read_connected.into(), cluster_handler, peer_id };
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(listener.listen(kill_switch));

        ListeningActorKillTrigger::new(kill_trigger, handle)
    }

    async fn read_command(&mut self) -> anyhow::Result<Vec<PeerMessage>> {
        self.read_connected
            .read_values()
            .await?
            .into_iter()
            .map(PeerMessage::try_from)
            .collect::<Result<_, _>>()
    }
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.start() => self.read_connected.0,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.0
        };
        connected
    }
    async fn start(&mut self) {
        while let Ok(cmds) = self.read_command().await {
            #[cfg(test)]
            ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for cmd in cmds {
                let _ = self
                    .cluster_handler
                    .send(PeerCommand { from: self.peer_id.clone(), msg: cmd })
                    .await;
            }
        }
    }
}
