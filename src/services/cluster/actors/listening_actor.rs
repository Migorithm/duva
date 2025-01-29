/// PeerListeningActor is responsible for listening to incoming messages from a peer.
/// Message from a peer is one of events that can trigger a change in the cluster state.
/// As it has to keep listening to incoming messages, it is implemented as an actor, run in the background.
/// To take a control of the actor, PeerListenerHandler is used, which can kill the listening process and return the connected stream.
use super::command::{ClusterCommand, CommandFromMaster, CommandFromSlave};
use super::peer::ReadConnected;
use super::types::PeerIdentifier;
use crate::services::cluster::actors::types::PeerKind;
use crate::services::interface::TRead;
use crate::services::query_io::QueryIO;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

pub(crate) struct PeerListeningActor {
    pub(super) read_connected: ReadConnected,
    pub(super) cluster_handler: Sender<ClusterCommand>,
    pub(crate) self_id: PeerIdentifier, // cluster_handler is used to send messages to the cluster actor
}

impl PeerListeningActor {
    /// Run until the kill switch is triggered
    /// returns the connected stream when the kill switch is triggered
    pub(super) async fn listen(mut self, rx: ReactorKillSwitch) -> ReadConnected {
        let connected = select! {
            _ = async{
                    match self.read_connected.kind {
                        PeerKind::Peer => {
                            self.listen_peer_stream().await
                        },
                        PeerKind::Replica => {
                            self.listen_replica_stream().await
                        },
                        PeerKind::Master => {
                            self.listen_master_stream().await
                        },
                    };
                } => {
                    self.read_connected
                },
            _ = rx => {
                // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
                self.read_connected
            }
        };
        connected
    }

    async fn listen_replica_stream(&mut self) {
        while let Ok(cmds) = self.read_command::<CommandFromSlave>().await {
            for cmd in cmds {
                match cmd {
                    CommandFromSlave::HeartBeat(peer_state) => {
                        println!("[INFO] Received peer state from slave");
                        // TODO update peer state on cluster manager
                    }
                }
            }
        }
    }
    async fn listen_peer_stream(&mut self) {
        while let Ok(values) = self.read_connected.stream.read_values().await {
            let _ = values;
        }
    }
    async fn listen_master_stream(&mut self) {
        while let Ok(cmds) = self.read_command::<CommandFromMaster>().await {
            for cmd in cmds {
                match cmd {
                    CommandFromMaster::HeartBeat(peer_state) => {
                        println!("[INFO] Received peer state from master");
                        self.cluster_handler
                            .send(ClusterCommand::ReportAlive {
                                peer_identifier: self.self_id.clone(),
                                state: peer_state,
                            })
                            .await
                            .unwrap();
                    }

                    CommandFromMaster::Replicate { query: _ } => {}
                    CommandFromMaster::Sync(v) => {
                        println!("[INFO] Received sync from master {:?}", v);
                    }
                }
            }
        }
    }

    async fn read_command<T>(&mut self) -> anyhow::Result<Vec<T>>
    where
        T: std::convert::TryFrom<QueryIO>,
        T::Error: Into<anyhow::Error>,
    {
        self.read_connected
            .stream
            .read_values()
            .await?
            .into_iter()
            .map(T::try_from)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }
}

pub(super) type KillTrigger = tokio::sync::oneshot::Sender<()>;
pub(super) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

#[derive(Debug)]
pub(crate) struct ListeningActorKillTrigger(KillTrigger, JoinHandle<ReadConnected>);
impl ListeningActorKillTrigger {
    pub(super) fn new(kill_trigger: KillTrigger, listning_task: JoinHandle<ReadConnected>) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(super) async fn kill(self) -> ReadConnected {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}
