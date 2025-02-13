/// PeerListeningActor is responsible for listening to incoming messages from a peer.
/// Message from a peer is one of events that can trigger a change in the cluster state.
/// As it has to keep listening to incoming messages, it is implemented as an actor, run in the background.
/// To take a control of the actor, PeerListenerHandler is used, which can kill the listening process and return the connected stream.
use crate::services::cluster::command::cluster_command::ClusterCommand;

use super::listening_command::{CommandFromMaster, CommandFromSlave};
use crate::services::cluster::peers::connected_types::ReadConnected;
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::peers::peer::ListeningActorKillTrigger;
use crate::services::cluster::replications::replication::HeartBeatMessage;
use crate::services::interface::TRead;
use crate::services::query_io::QueryIO;
use tokio::select;
use tokio::sync::mpsc::Sender;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
pub(crate) struct PeerListeningActor {
    pub(crate) read_connected: ReadConnected,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
    pub(crate) self_id: PeerIdentifier,
}

impl PeerListeningActor {
    pub fn new(
        read_connected: ReadConnected,
        cluster_handler: Sender<ClusterCommand>,
        self_id: PeerIdentifier,
    ) -> ListeningActorKillTrigger {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let listening_actor = PeerListeningActor { read_connected, cluster_handler, self_id };

        ListeningActorKillTrigger::new(
            kill_trigger,
            tokio::spawn(listening_actor.listen(kill_switch)),
        )
    }

    // Update peer state on cluster manager
    async fn receive_heartbeat(&mut self, state: HeartBeatMessage) {
        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
        let _ = self.cluster_handler.send(ClusterCommand::ReceiveHeartBeat(state)).await;
    }

    /// Run until the kill switch is triggered
    /// returns the connected stream when the kill switch is triggered
    pub(crate) async fn listen(mut self, rx: ReactorKillSwitch) -> ReadConnected {
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
                } => self.read_connected,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected
        };
        connected
    }

    async fn listen_replica_stream(&mut self) {
        while let Ok(cmds) = self.read_command::<CommandFromSlave>().await {
            for cmd in cmds {
                match cmd {
                    CommandFromSlave::HeartBeat(state) => {
                        self.receive_heartbeat(state).await;
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
                    CommandFromMaster::HeartBeat(mut state) => {
                        self.log_entries(&mut state);
                        self.receive_heartbeat(state).await;
                    }
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

    fn log_entries(&self, state: &mut HeartBeatMessage) {
        let append_entries = state.append_entries.drain(..).collect::<Vec<_>>();
        for entry in append_entries {
            println!("[INFO] Append entry {:?}", entry);
        }
    }
}

pub(super) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;
