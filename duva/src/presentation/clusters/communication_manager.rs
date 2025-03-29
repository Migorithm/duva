use crate::{
    domains::{
        append_only_files::WriteOperation,
        cluster_actors::{commands::ClusterCommand, replication::ReplicationState},
        peers::identifier::PeerIdentifier,
    },
    make_smart_pointer,
};

use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub(crate) struct ClusterCommunicationManager(pub(crate) Sender<ClusterCommand>);

make_smart_pointer!(ClusterCommunicationManager, Sender<ClusterCommand>);

impl ClusterCommunicationManager {
    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerIdentifier>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn replication_info(&self) -> anyhow::Result<ReplicationState> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::ReplicationInfo(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn cluster_info(&self) -> anyhow::Result<String> {
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
        let known_node_len = self.get_peers().await?.len();
        Ok(format!("cluster_known_nodes:{}", known_node_len))
    }

    pub(crate) async fn forget_peer(
        &self,
        peer_identifier: PeerIdentifier,
    ) -> anyhow::Result<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Option<()>>();
        self.send(ClusterCommand::ForgetPeer(peer_identifier, tx)).await?;
        let Some(_) = rx.await? else { return Ok(false) };
        Ok(true)
    }

    pub(crate) async fn replicaof(&self, peer_identifier: PeerIdentifier) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.send(ClusterCommand::ReplicaOf(peer_identifier, tx)).await;

        let _ = rx.await;
    }

    pub(crate) async fn cluster_nodes(&self) -> anyhow::Result<Vec<String>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::ClusterNodes(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn fetch_logs_for_sync(&self) -> anyhow::Result<Vec<WriteOperation>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::FetchCurrentState(tx)).await?;
        Ok(rx.await?)
    }
}
