use crate::{
    domains::{
        cluster_actors::{commands::ClusterCommand, replication::ReplicationInfo, ClusterActor},
        peers::identifier::PeerIdentifier,
    },
    make_smart_pointer,
};
use std::time::Duration;
use tokio::{sync::mpsc::Sender, time::interval};

#[derive(Clone)]
pub struct ClusterCommunicationManager(pub(crate) Sender<ClusterCommand>);

make_smart_pointer!(ClusterCommunicationManager, Sender<ClusterCommand>);

impl ClusterCommunicationManager {
    pub fn run(
        notifier: tokio::sync::watch::Sender<bool>,
        node_timeout: u128,
        heartbeat_fq_mills: u64,
        replicaof: Option<(String, String)>,
        host: String,
        port: u16,
    ) -> Sender<ClusterCommand> {
        let (actor_handler, cluster_message_listener) = tokio::sync::mpsc::channel(100);

        tokio::spawn(
            ClusterActor::new(node_timeout, ReplicationInfo::new(replicaof, &host, port), notifier)
                .handle(cluster_message_listener),
        );

        tokio::spawn({
            let heartbeat_sender = actor_handler.clone();
            let mut heartbeat_interval = interval(Duration::from_millis(heartbeat_fq_mills));
            async move {
                loop {
                    heartbeat_interval.tick().await;
                    let _ = heartbeat_sender.send(ClusterCommand::SendHeartBeat).await;
                }
            }
        });

        // TODO peer state may need to be picked up from a persistent storage on restart case
        actor_handler
    }

    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerIdentifier>> {
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
