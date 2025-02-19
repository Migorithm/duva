use super::communication_manager::ClusterCommunicationManager;
use super::outbound::stream::OutboundStream;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::kind::PeerKind;

use crate::services::cluster::actors::replication::IS_LEADER_MODE;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;
use crate::{make_smart_pointer, InboundStream};
use tokio::sync::mpsc::Sender;

pub struct ClusterConnectionManager(pub(crate) ClusterCommunicationManager);

impl ClusterConnectionManager {
    pub fn new(actor_handler: Sender<ClusterCommand>) -> Self {
        Self(ClusterCommunicationManager(actor_handler))
    }

    pub(crate) async fn accept_inbound_stream(
        &self,
        mut peer_stream: InboundStream,
        cache_manager: CacheManager,
        snapshot_applier: SnapshotApplier,
    ) -> anyhow::Result<()> {
        peer_stream.recv_threeway_handshake().await?;

        peer_stream.disseminate_peers(self.0.get_peers().await?).await?;

        if matches!(peer_stream.peer_kind()?, PeerKind::Follower)
            && IS_LEADER_MODE.load(std::sync::atomic::Ordering::Acquire)
        {
            peer_stream = peer_stream.send_sync_to_inbound_server(cache_manager).await?;
        }
        self.send(peer_stream.to_add_peer(self.clone(), snapshot_applier)?).await?;

        Ok(())
    }

    pub(crate) async fn discover_cluster(
        self,
        self_port: u16,
        connect_to: PeerIdentifier,
        snapshot_applier: SnapshotApplier,
    ) -> anyhow::Result<()> {
        // Base case
        let existing_peers = self.get_peers().await?;
        if existing_peers.contains(&connect_to) {
            return Ok(());
        }

        // Recursive case
        let (add_peer_cmd, connected_node_info) =
            OutboundStream::new(connect_to, self.replication_info().await?.leader_repl_id)
                .await?
                .establish_connection(self_port)
                .await?
                .set_replication_info(&self)
                .await?
                .create_peer_cmd(self.clone(), snapshot_applier.clone())?;
        self.send(add_peer_cmd).await?;

        // Discover additional peers concurrently
        // TODO Require investigation. Why does 'list_peer_binding_addrs' have to be called at here?
        for peer in connected_node_info.list_peer_binding_addrs() {
            println!("Discovering peer: {}", peer);
            Box::pin(ClusterConnectionManager(self.0.clone()).discover_cluster(
                self_port,
                peer,
                snapshot_applier.clone(),
            ))
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

    pub fn to_communication_manager(self) -> ClusterCommunicationManager {
        self.0
    }
}

make_smart_pointer!(ClusterConnectionManager, ClusterCommunicationManager);
