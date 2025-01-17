use super::actors::actor::ClusterActor;
use super::actors::command::ClusterCommand;
use super::actors::replication::Replication;
use super::actors::types::{PeerAddr, PeerKind};
use crate::make_smart_pointer;
use crate::services::cluster::inbound::stream::InboundStream;
use crate::services::cluster::outbound::stream::OutboundStream;

use crate::services::interface::TStream;
use crate::services::query_io::QueryIO;

use std::time::Duration;
use tokio::net::TcpStream;
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
                    let _ = heartbeat_sender.send(ClusterCommand::ping()).await;
                }
            }
        });
        Self(actor_handler)
    }

    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerAddr>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn accept_peer(&self, mut peer_stream: InboundStream) -> anyhow::Result<()> {
        let repl_info = self.replication_info().await?;

        let (peer_addr, master_repl_id) = peer_stream.recv_threeway_handshake(&repl_info).await?;

        // TODO Need to decide which point to send file data
        // TODO At this point, slave stream must write master_replid so that other nodes can tell where it belongs

        self.disseminate_peers(&mut peer_stream).await?;

        // TODO At this point again, slave tries to connect to other nodes as peer in the cluster
        self.send(ClusterCommand::AddPeer {
            peer_addr,
            stream: peer_stream.0,
            peer_kind: PeerKind::accepted_peer_kind(&repl_info.master_replid, &master_repl_id),
        })
        .await?;
        Ok(())
    }

    async fn disseminate_peers(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let peers = self.get_peers().await?;
        stream
            .write(QueryIO::SimpleString(format!(
                "PEERS {}",
                peers.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" ")
            )))
            .await?;
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
        connect_to: String,
    ) -> anyhow::Result<()> {
        let existing_peers = self.get_peers().await?;
        let repl_info = self.replication_info().await?;

        // Base case
        if existing_peers.contains(&PeerAddr(connect_to.clone())) {
            return Ok(());
        }

        let mut outbound_stream = OutboundStream(TcpStream::connect(&connect_to).await?);

        let connection_info = outbound_stream.establish_connection(self_port).await?;

        self.send(ClusterCommand::AddPeer {
            peer_addr: PeerAddr(connect_to),
            stream: outbound_stream.0,
            peer_kind: PeerKind::connected_peer_kind(&repl_info, &connection_info.repl_id),
        })
        .await?;

        if repl_info.master_replid == "?" {
            self.send(ClusterCommand::SetReplicationId(connection_info.repl_id.clone())).await?;
        }

        for peer in connection_info.list_peer_binding_addrs() {
            // Recursive async calls need indirection because the compiler needs to know the size of the Future at compile time.
            // async fn returns an anonymous Future type that contains all the state needed to execute the async function. With recursion, this would create an infinitely nested type like:
            // Future<Future<Future<...>>>
            // Using Box::pin adds a layer of indirection through the heap, breaking the infinite size issue by making the Future size fixed
            Box::pin(self.discover_cluster(self_port, peer)).await?;
        }

        Ok(())
    }
}
