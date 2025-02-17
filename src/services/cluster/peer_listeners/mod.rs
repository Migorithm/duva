use tokio::{net::tcp::OwnedReadHalf, sync::mpsc::Sender};

use crate::services::{
    interface::TRead, query_io::QueryIO, statefuls::snapshot::snapshot_applier::SnapshotApplier,
};

use super::{
    actors::commands::ClusterCommand,
    peers::{connected_types::ReadConnected, identifier::PeerIdentifier},
    replications::replication::HeartBeatMessage,
};
use tokio::select;

pub mod follower;
pub mod leader;
pub mod peer;

pub trait TListen {
    fn listen(
        self,
        rx: ReactorKillSwitch,
    ) -> impl std::future::Future<Output = OwnedReadHalf> + Send;
}

pub(super) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
#[derive(Debug)]
pub(crate) struct PeerListener<T> {
    pub(crate) read_connected: ReadConnected<T>,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
    pub(crate) self_id: PeerIdentifier,
    pub(crate) snapshot_applier: SnapshotApplier,
}
impl<T> PeerListener<T> {
    pub fn new(
        read_connected: ReadConnected<T>,
        cluster_handler: Sender<ClusterCommand>,
        self_id: PeerIdentifier,
        snapshot_applier: SnapshotApplier,
    ) -> Self {
        Self { read_connected, cluster_handler, self_id, snapshot_applier }
    }
    // Update peer state on cluster manager
    async fn receive_heartbeat(&mut self, state: HeartBeatMessage) {
        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
        let _ = self.cluster_handler.send(ClusterCommand::ReceiveHeartBeat(state)).await;
    }
    async fn log_entries(&self, state: &mut HeartBeatMessage) {
        let append_entries = state.append_entries.drain(..).collect::<Vec<_>>();
        if append_entries.is_empty() {
            return;
        }

        let _ = self
            .cluster_handler
            .send(ClusterCommand::FollowerReceiveLogEntries(append_entries))
            .await;
    }

    async fn read_command<U>(&mut self) -> anyhow::Result<Vec<U>>
    where
        U: std::convert::TryFrom<QueryIO>,
        U::Error: Into<anyhow::Error>,
    {
        self.read_connected
            .stream
            .read_values()
            .await?
            .into_iter()
            .map(U::try_from)
            .collect::<Result<_, _>>()
            .map_err(Into::into)
    }
}
