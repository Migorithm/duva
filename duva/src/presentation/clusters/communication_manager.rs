use crate::domains::cluster_actors::topology::Topology;
use crate::{
    domains::{
        cluster_actors::{
            ClientMessage, ConnectionMessage, LazyOption,
            actor::ClusterCommandHandler,
            replication::{ReplicationRole, ReplicationState},
        },
        peers::{identifier::PeerIdentifier, peer::PeerState},
    },
    make_smart_pointer,
};

#[derive(Clone, Debug)]
pub(crate) struct ClusterCommunicationManager(pub(crate) ClusterCommandHandler);

make_smart_pointer!(ClusterCommunicationManager, ClusterCommandHandler);

impl ClusterCommunicationManager {
    pub(crate) async fn route_get_peers(&self) -> anyhow::Result<Vec<PeerIdentifier>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClientMessage::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn route_get_topology(&self) -> anyhow::Result<Topology> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClientMessage::GetTopology(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn route_connect_to_server(
        &self,
        connect_to: PeerIdentifier,
    ) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ConnectionMessage::ConnectToServer { connect_to, callback: tx }).await?;
        rx.await??;
        Ok(())
    }

    pub(crate) async fn route_get_replication_state(&self) -> anyhow::Result<ReplicationState> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClientMessage::ReplicationInfo(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn route_get_cluster_info(&self) -> anyhow::Result<String> {
        //cluster_state:ok
        //cluster_slots_assigned:16384
        //cluster_slots_ok:16384
        //cluster_slots_pfail:0
        //cluster_slots_fail:0
        //cluster_known_nodes:6
        //cluster_size:3
        //cluster_current_epoch:6
        //cluster_my_epoch:2
        //cluster_stats_messages_sent:1483972
        //cluster_stats_messages_received:1483968
        //total_cluster_links_buffer_limit_exceeded:0
        let known_node_len = self.route_get_peers().await?.len();
        Ok(format!("cluster_known_nodes:{known_node_len}"))
    }

    pub(crate) async fn route_forget_peer(
        &self,
        peer_identifier: PeerIdentifier,
    ) -> anyhow::Result<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Option<()>>();
        self.send(ClientMessage::ForgetPeer(peer_identifier, tx)).await?;
        let Some(_) = rx.await? else { return Ok(false) };
        Ok(true)
    }

    pub(crate) async fn route_replicaof(
        &self,
        peer_identifier: PeerIdentifier,
    ) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ClientMessage::ReplicaOf(peer_identifier, tx)).await;

        rx.await?
    }

    pub(crate) async fn route_cluster_meet(
        &self,
        peer_identifier: PeerIdentifier,
        lazy_option: LazyOption,
    ) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ClientMessage::ClusterMeet(peer_identifier, lazy_option, tx)).await;
        rx.await?
    }
    pub(crate) async fn route_cluster_reshard(&self) -> anyhow::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ClientMessage::ClusterReshard(tx)).await;
        rx.await?
    }

    pub(crate) async fn route_cluster_nodes(&self) -> anyhow::Result<Vec<PeerState>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClientMessage::ClusterNodes(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn route_get_role(&self) -> anyhow::Result<ReplicationRole> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClientMessage::GetRole(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn route_subscribe_topology_change(
        &self,
    ) -> anyhow::Result<tokio::sync::broadcast::Receiver<Topology>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ClientMessage::SubscribeToTopologyChange(tx)).await;
        Ok(rx.await?)
    }
}
