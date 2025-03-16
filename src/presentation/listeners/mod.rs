use crate::domains::{
    cluster_actors::{commands::ClusterCommand, replication::HeartBeatMessage},
    peers::{connected_types::ReadConnected, identifier::PeerIdentifier},
    query_parsers::QueryIO,
};
use tokio::net::tcp::OwnedReadHalf;
use tokio::select;

pub mod peer_listener;

use crate::{presentation::listeners::peer_listener::PeerInput, services::interface::TRead};

use tokio::sync::mpsc::Sender;
pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
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
