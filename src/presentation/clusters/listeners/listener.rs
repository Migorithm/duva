use super::*;
use crate::domains::cluster_actors::commands::ClusterCommand;

#[cfg(test)]
static ATOMIC: std::sync::atomic::AtomicI16 = std::sync::atomic::AtomicI16::new(0);

#[derive(Debug)]
pub(crate) struct ClusterListener {
    pub(crate) read_connected: ReadConnected,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
    pub(crate) listening_to: PeerIdentifier,
}

impl ClusterListener {
    pub fn new(
        read_connected: ReadConnected,
        cluster_handler: Sender<ClusterCommand>,
        listening_to: PeerIdentifier,
    ) -> Self {
        Self { read_connected, cluster_handler, listening_to }
    }

    pub(crate) async fn read_command(&mut self) -> anyhow::Result<Vec<PeerInput>> {
        self.read_connected
            .stream
            .read_values()
            .await?
            .into_iter()
            .map(PeerInput::try_from)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
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
            #[cfg(test)]
            ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for cmd in cmds {
                match cmd {
                    PeerInput::AppendEntriesRPC(hb) => {
                        println!("[INFO] from {}, hc:{}", hb.from, hb.hop_count);
                        let _ =
                            self.cluster_handler.send(ClusterCommand::AppendEntriesRPC(hb)).await;
                    },
                    PeerInput::ClusterHeartBeat(hb) => {
                        println!("[INFO] from {}, hc:{}", hb.from, hb.hop_count);
                        let _ =
                            self.cluster_handler.send(ClusterCommand::ClusterHeartBeat(hb)).await;
                    },
                    PeerInput::FullSync(logs) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::InstallLeaderState(logs))
                            .await;
                    },
                    PeerInput::Acks(items) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::ReplicationResponse(items))
                            .await;
                    },
                    PeerInput::RequestVote(request_vote) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::VoteElection(request_vote))
                            .await;
                    },
                    PeerInput::RequestVoteReply(reply) => {
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
