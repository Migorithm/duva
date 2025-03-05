use super::communication_manager::ClusterCommunicationManager;
use super::outbound::stream::OutboundStream;
use crate::domains::caches::cache_manager::CacheManager;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::replication::IS_LEADER_MODE;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::kind::PeerKind;
use crate::domains::saves::snapshot::snapshot_applier::SnapshotApplier;
use crate::{InboundStream, make_smart_pointer};
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

        if let PeerKind::Follower { hwm, leader_repl_id } = peer_stream.peer_kind()? {
            if IS_LEADER_MODE.load(std::sync::atomic::Ordering::Acquire) && hwm == 0 {
                peer_stream = peer_stream.send_full_resync_to_inbound_server(cache_manager).await?;
            }
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
        let replication_info = self.replication_info().await?;

        let (add_peer_cmd, peer_list) = OutboundStream::new(connect_to, replication_info)
            .await?
            .establish_connection(self_port)
            .await?
            .set_replication_info(&self)
            .await?
            .create_peer_cmd(self.clone(), snapshot_applier.clone())?;
        self.send(add_peer_cmd).await?;

        // Discover additional peers concurrently
        // TODO Require investigation. Why does 'list_peer_binding_addrs' have to be called at here?
        for peer in peer_list {
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
        self.0.0.clone()
    }

    pub async fn send(&self, cmd: ClusterCommand) -> anyhow::Result<()> {
        Ok(self.0.send(cmd).await?)
    }

    pub fn to_communication_manager(self) -> ClusterCommunicationManager {
        self.0
    }
}

make_smart_pointer!(ClusterConnectionManager, ClusterCommunicationManager);
