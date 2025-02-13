use crate::services::cluster::actors::commands::ClusterCommand;
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::replications::replication::IS_MASTER_MODE;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::{make_smart_pointer, InboundStream};
use tokio::sync::mpsc::Sender;

use super::communication_manager::ClusterCommunicationManager;
use super::outbound::stream::OutboundStream;

pub struct ClusterConnectionManager(pub(crate) ClusterCommunicationManager);

impl ClusterConnectionManager {
    pub fn new(actor_handler: Sender<ClusterCommand>) -> Self {
        Self(ClusterCommunicationManager(actor_handler))
    }

    pub(crate) async fn accept_inbound_stream(
        &self,
        mut peer_stream: InboundStream,
        cache_manager: CacheManager,
    ) -> anyhow::Result<()> {
        peer_stream.recv_threeway_handshake().await?;

        peer_stream.disseminate_peers(self.0.get_peers().await?).await?;

        if matches!(peer_stream.peer_kind()?, PeerKind::Replica)
            && IS_MASTER_MODE.load(std::sync::atomic::Ordering::Acquire)
        {
            peer_stream = peer_stream.send_sync_to_inbound_server(cache_manager).await?;
        }
        self.send(peer_stream.to_add_peer(self.clone())?).await?;

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
        let (add_peer_cmd, connected_node_info) =
            OutboundStream::new(connect_to, self.replication_info().await?)
                .await?
                .establish_connection(self_port)
                .await?
                .set_replication_info(&self)
                .await?
                .deconstruct(self.clone())?;
        self.send(add_peer_cmd).await?;

        // Discover additional peers concurrently
        // TODO Require investigation. Why does 'list_peer_binding_addrs' have to be called at here?
        for peer in connected_node_info.list_peer_binding_addrs() {
            println!("Discovering peer: {}", peer);
            Box::pin(ClusterConnectionManager(self.0.clone()).discover_cluster(self_port, peer))
                .await?;
        }

        Ok(())
    }

    pub fn clone(&self) -> Sender<ClusterCommand> {
        self.0 .0.clone()
    }

    pub async fn send(&self, cmd: ClusterCommand) -> anyhow::Result<()> {
        Ok(self.0.send(cmd).await?)
    }
}

make_smart_pointer!(ClusterConnectionManager, ClusterCommunicationManager);
