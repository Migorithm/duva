use super::actors::actor::ClusterActor;
use super::actors::command::ClusterCommand;
use super::actors::replication::{Replication, IS_MASTER_MODE};
use super::actors::types::{PeerAddr, PeerAddrs, PeerKind};
use crate::make_smart_pointer;
use crate::services::cluster::inbound::stream::InboundStream;
use crate::services::cluster::outbound::stream::OutboundStream;
use crate::services::statefuls::cache::manager::CacheManager;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::interval;

#[derive(Clone)]
pub struct ClusterManager(Sender<ClusterCommand>);
make_smart_pointer!(ClusterManager, Sender<ClusterCommand>);

impl ClusterManager {
    pub fn run(notifier: tokio::sync::watch::Sender<bool>) -> Self {
        let (actor_handler, cluster_message_listener) = tokio::sync::mpsc::channel(100);
        tokio::spawn(ClusterActor::default().handle(
            actor_handler.clone(),
            cluster_message_listener,
            notifier,
        ));

        tokio::spawn({
            let heartbeat_sender = actor_handler.clone();
            async move {
                let mut interval = interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let _ = heartbeat_sender.send(ClusterCommand::Ping).await;
                }
            }
        });
        Self(actor_handler)
    }

    pub(crate) async fn get_peers(&self) -> anyhow::Result<PeerAddrs> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn accept_inbound_stream(
        &self,
        mut peer_stream: InboundStream,
        cache_manager: &'static CacheManager,
    ) -> anyhow::Result<()> {
        peer_stream.recv_threeway_handshake().await?;

        peer_stream.disseminate_peers(self.get_peers().await?).await?;

        if matches!(peer_stream.peer_kind()?, PeerKind::Replica)
            && IS_MASTER_MODE.load(std::sync::atomic::Ordering::Acquire)
        {
            peer_stream.send_sync_to_inbound_server(cache_manager).await?;
        }
        self.send(peer_stream.to_add_peer()?).await?;

        Ok(())
    }

    pub(crate) async fn replication_info(&self) -> anyhow::Result<Replication> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::ReplicationInfo(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn discover_cluster(
        &'static self,
        self_port: u16,
        connect_to: PeerAddr,
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
                .set_replication_id(self)
                .await?
                .deconstruct()?;
        self.send(add_peer_cmd).await?;

        // Discover additional peers concurrently
        for peer in connected_node_info.list_peer_binding_addrs() {
            // TODO Require investigation. Why does this have to be called at here?
            Box::pin(self.discover_cluster(self_port, peer)).await?;
        }

        Ok(())
    }
}
