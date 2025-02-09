use super::actors::actor::ClusterActor;
use crate::services::client::request::ClientRequest;
use crate::services::cluster::command::cluster_command::ClusterCommand;
use crate::services::cluster::inbound::stream::InboundStream;
use crate::services::cluster::outbound::stream::OutboundStream;
use crate::services::cluster::peers::address::PeerAddrs;
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::cluster::replications::replication::{ReplicationInfo, IS_MASTER_MODE};
use crate::services::statefuls::cache::manager::CacheManager;
use crate::{get_env, make_smart_pointer};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::interval;

#[derive(Clone)]
pub struct ClusterManager {
    actor_handler: Sender<ClusterCommand>,
}
make_smart_pointer!(ClusterManager, Sender<ClusterCommand> => actor_handler);

impl ClusterManager {
    pub fn run(notifier: tokio::sync::watch::Sender<bool>) -> Self {
        let (actor_handler, cluster_message_listener) = tokio::sync::mpsc::channel(100);
        tokio::spawn(ClusterActor::new(get_env().ttl_mills).handle(
            actor_handler.clone(),
            cluster_message_listener,
            notifier,
        ));

        tokio::spawn({
            let heartbeat_sender = actor_handler.clone();
            let mut heartbeat_interval = interval(Duration::from_millis(get_env().hf_mills));
            async move {
                loop {
                    heartbeat_interval.tick().await;
                    let _ = heartbeat_sender.send(ClusterCommand::SendHeartBeat).await;
                }
            }
        });

        // TODO peer state may need to be picked up from a persistent storage on restart case
        Self { actor_handler }
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
            peer_stream = peer_stream.send_sync_to_inbound_server(cache_manager).await?;
        }
        self.send(peer_stream.to_add_peer()?).await?;

        Ok(())
    }

    pub(crate) async fn replication_info(&self) -> anyhow::Result<ReplicationInfo> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::ReplicationInfo(tx)).await?;
        Ok(rx.await?)
    }

    pub(crate) async fn discover_cluster(
        &'static self,
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
                .set_replication_info(self)
                .await?
                .deconstruct()?;
        self.send(add_peer_cmd).await?;

        // Discover additional peers concurrently
        // TODO Require investigation. Why does 'list_peer_binding_addrs' have to be called at here?
        for peer in connected_node_info.list_peer_binding_addrs() {
            println!("Discovering peer: {}", peer);
            Box::pin(self.discover_cluster(self_port, peer)).await?;
        }

        Ok(())
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

    pub(crate) async fn log_request(&self, request: &ClientRequest) -> anyhow::Result<()> {
        Ok(())
    }

    pub(crate) async fn concensus(
        &self,
        log: crate::services::query_io::QueryIO,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::Concensus { log, sender: tx }).await;
        rx
    }
}
