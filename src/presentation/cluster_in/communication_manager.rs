use tokio::sync::mpsc::Sender;

use crate::{
    make_smart_pointer,
    services::cluster::{
        command::cluster_command::ClusterCommand,
        peers::{address::PeerAddrs, identifier::PeerIdentifier},
        replications::replication::ReplicationInfo,
    },
};

#[derive(Clone)]
pub struct ClusterCommunicationManager {
    actor_handler: Sender<ClusterCommand>,
}

make_smart_pointer!(ClusterCommunicationManager, Sender<ClusterCommand> => actor_handler);

impl ClusterCommunicationManager {
    pub(crate) async fn get_peers(&self) -> anyhow::Result<PeerAddrs> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn replication_info(&self) -> anyhow::Result<ReplicationInfo> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::ReplicationInfo(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn cluster_info(&self) -> anyhow::Result<Vec<String>> {
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
        Ok(vec![format!("cluster_known_nodes:{}", known_node_len)])
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
}
