use crate::{
    domains::{
        cluster_actors::commands::{ClusterCommand, PeerListenerCommand},
        peers::{connected_types::ReadConnected, peer::ListeningActorKillTrigger},
    },
    prelude::PeerIdentifier,
    services::interface::TRead,
};
use tokio::{net::tcp::OwnedReadHalf, select, sync::mpsc::Sender};
use tracing::{debug, trace};

#[cfg(test)]
static ATOMIC: std::sync::atomic::AtomicI16 = std::sync::atomic::AtomicI16::new(0);

pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

#[derive(Debug)]
pub(crate) struct PeerListener {
    pub(crate) read_connected: ReadConnected,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
    pub(crate) listening_to: PeerIdentifier,
}

impl PeerListener {
    pub(crate) fn spawn(
        read_connected: OwnedReadHalf,
        cluster_handler: Sender<ClusterCommand>,
        listening_to: PeerIdentifier,
    ) -> ListeningActorKillTrigger {
        let listener = Self {
            read_connected: ReadConnected::new(read_connected),
            cluster_handler,
            listening_to,
        };
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(listener.listen(kill_switch));

        ListeningActorKillTrigger::new(kill_trigger, handle)
    }

    pub(crate) async fn read_command(&mut self) -> anyhow::Result<Vec<PeerListenerCommand>> {
        self.read_connected
            .stream
            .read_values()
            .await?
            .into_iter()
            .map(PeerListenerCommand::try_from)
            .collect::<Result<_, _>>()
    }
    pub(crate) async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_peer() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
    async fn listen_peer(&mut self) {
        while let Ok(cmds) = self.read_command().await {
            trace!(?cmds, "Received command from peer");
            #[cfg(test)]
            ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for cmd in cmds {
                match cmd {
                    PeerListenerCommand::AppendEntriesRPC(hb) => {
                        debug!("from {}, hc:{}", hb.from, hb.hop_count);
                        let _ =
                            self.cluster_handler.send(ClusterCommand::AppendEntriesRPC(hb)).await;
                    },
                    PeerListenerCommand::ClusterHeartBeat(hb) => {
                        debug!("from {}, hc:{}", hb.from, hb.hop_count);
                        let _ =
                            self.cluster_handler.send(ClusterCommand::ClusterHeartBeat(hb)).await;
                    },
                    PeerListenerCommand::Acks(items) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::ReplicationResponse(items))
                            .await;
                    },
                    PeerListenerCommand::RequestVote(request_vote) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::VoteElection(request_vote))
                            .await;
                    },
                    PeerListenerCommand::RequestVoteReply(reply) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::ApplyElectionVote(reply))
                            .await;
                    },
                }
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self
            .cluster_handler
            .send(ClusterCommand::ForgetPeer(self.listening_to.clone(), tx))
            .await;
        rx.await.ok();
    }
}
