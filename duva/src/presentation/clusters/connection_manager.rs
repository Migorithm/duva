use super::communication_manager::ClusterCommunicationManager;
use super::outbound::stream::OutboundStream;

use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::{InboundStream, make_smart_pointer};
use tokio::sync::mpsc::Sender;

pub(crate) struct ClusterConnectionManager(pub(crate) ClusterCommunicationManager);

impl ClusterConnectionManager {
    pub(crate) async fn accept_inbound_stream(
        &self,
        mut peer_stream: InboundStream,
        ccm: ClusterCommunicationManager,
    ) -> anyhow::Result<()> {
        let connected_peer_info = peer_stream.recv_threeway_handshake().await?;

        peer_stream.disseminate_peers(self.0.get_peers().await?).await?;
        peer_stream.may_try_sync(ccm, &connected_peer_info).await?;

        self.send(peer_stream.into_add_peer(self.clone(), connected_peer_info)?).await?;

        Ok(())
    }

    pub(crate) async fn discover_cluster(
        self,
        self_port: u16,
        connect_to: PeerIdentifier,
    ) -> anyhow::Result<()> {
        // Base case
        let existing_peers = self.get_peers().await?;
        if existing_peers.contains(&connect_to) {
            return Ok(());
        }

        // Recursive case
        let replication_info = self.replication_info().await?;

        let (add_peer_cmd, peer_list) = OutboundStream::new(connect_to, replication_info)
            .await?
            .establish_connection(self_port)
            .await?
            .set_replication_info(&self)
            .await?
            .create_peer_cmd(self.clone())?;
        self.send(add_peer_cmd).await?;

        // Discover additional peers concurrently
        // TODO Require investigation. Why does 'list_peer_binding_addrs' have to be called at here?
        for peer in peer_list {
            println!("Discovering peer: {}", peer);
            Box::pin(ClusterConnectionManager(self.0.clone()).discover_cluster(self_port, peer))
                .await?;
        }

        Ok(())
    }

    pub fn clone(&self) -> Sender<ClusterCommand> {
        self.0.0.clone()
    }

    pub async fn send(&self, cmd: ClusterCommand) -> anyhow::Result<()> {
        Ok(self.0.send(cmd).await?)
    }
}

make_smart_pointer!(ClusterConnectionManager, ClusterCommunicationManager);
