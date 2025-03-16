use super::{
    cluster_actors::{commands::ClusterCommand, replication::HeartBeatMessage},
    peers::connected_types::ReadConnected,
    query_parsers::QueryIO,
};
use crate::{presentation::listeners::leader::PeerInput, services::interface::TRead};

use tokio::sync::mpsc::Sender;
pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
#[derive(Debug)]
pub(crate) struct ClusterListener {
    pub(crate) read_connected: ReadConnected,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
}

impl ClusterListener {
    pub fn new(read_connected: ReadConnected, cluster_handler: Sender<ClusterCommand>) -> Self {
        Self { read_connected, cluster_handler }
    }
    // Update peer state on cluster manager
    pub(crate) async fn receive_heartbeat(&mut self, state: HeartBeatMessage) {
        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
        let _ = self.cluster_handler.send(ClusterCommand::ReceiveHeartBeat(state)).await;
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
}
