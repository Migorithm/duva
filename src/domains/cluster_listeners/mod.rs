pub mod interfaces;
use super::{
    cluster_actors::{commands::ClusterCommand, replication::HeartBeatMessage},
    peers::connected_types::ReadConnected,
    query_parsers::QueryIO,
};
use crate::services::interface::TRead;
pub(crate) use interfaces::TListen;
use tokio::sync::mpsc::Sender;
pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
#[derive(Debug)]
pub(crate) struct ClusterListener<T> {
    pub(crate) read_connected: ReadConnected<T>,
    pub(crate) cluster_handler: Sender<ClusterCommand>,
}

impl<T> ClusterListener<T> {
    pub fn new(
        read_connected: ReadConnected<T>,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Self {
        Self { read_connected, cluster_handler }
    }
    // Update peer state on cluster manager
    pub(crate) async fn receive_heartbeat(&mut self, state: HeartBeatMessage) {
        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
        let _ = self.cluster_handler.send(ClusterCommand::ReceiveHeartBeat(state)).await;
    }

    pub(crate) async fn read_command<U>(&mut self) -> anyhow::Result<Vec<U>>
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
