/// PeerListeningActor is responsible for listening to incoming messages from a peer.
/// Message from a peer is one of events that can trigger a change in the cluster state.
/// As it has to keep listening to incoming messages, it is implemented as an actor, run in the background.
/// To take a control of the actor, PeerListenerHandler is used, which can kill the listening process and return the connected stream.
use super::requests::{RequestFromMaster, RequestFromSlave};
use crate::services::cluster::actors::commands::ClusterCommand;
use crate::services::cluster::peers::connected_types::ReadConnected;
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::peers::peer::ListeningActorKillTrigger;
use crate::services::cluster::replications::replication::HeartBeatMessage;
use crate::services::interface::TRead;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;
use crate::services::statefuls::snapshot::snapshot_loader::SnapshotLoader;
use tokio::select;
use tokio::sync::mpsc::Sender;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
pub(crate) struct PeerListener {
    pub(crate) read_connected: ReadConnected,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
    pub(crate) self_id: PeerIdentifier,
    pub(crate) snapshot_applier: SnapshotApplier,
}

impl PeerListener {
    pub fn new(
        read_connected: ReadConnected,
        cluster_handler: Sender<ClusterCommand>,
        self_id: PeerIdentifier,
        snapshot_applier: SnapshotApplier,
    ) -> ListeningActorKillTrigger {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let listening_actor =
            PeerListener { read_connected, cluster_handler, self_id, snapshot_applier };

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
        while let Ok(cmds) = self.read_command::<RequestFromSlave>().await {
            for cmd in cmds {
                match cmd {
                    RequestFromSlave::HeartBeat(state) => {
                        self.receive_heartbeat(state).await;
                    }
                    RequestFromSlave::Acks(items) => {
                        let _ = self.cluster_handler.send(ClusterCommand::ReceiveAcks(items)).await;
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
        while let Ok(cmds) = self.read_command::<RequestFromMaster>().await {
            for cmd in cmds {
                match cmd {
                    RequestFromMaster::HeartBeat(mut state) => {
                        self.log_entries(&mut state);
                        self.receive_heartbeat(state).await;
                    }
                    RequestFromMaster::FullSync(data) => {
                        let Ok(snapshot) = SnapshotLoader::load_from_bytes(&data) else {
                            println!("[ERROR] Failed to load snapshot from master");
                            continue;
                        };
                        let Ok(_) = self.snapshot_applier.apply_snapshot(snapshot).await else {
                            println!("[ERROR] Failed to apply snapshot from master");
                            continue;
                        };
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
        let _ = self.cluster_handler.send(ClusterCommand::ReceiveLogEntries(append_entries));
    }
}

pub(super) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;
