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

    pub(crate) async fn accept_peer(&self, mut peer_stream: InboundStream) {
        let (peer_addr, master_repl_id) = peer_stream.recv_threeway_handshake().await.unwrap();

        let repl_info = self.replication_info().await.unwrap();

        // TODO Need to decide which point to send file data
        // TODO At this point, slave stream must write master_replid so that other nodes can tell where it belongs

        self.disseminate_peers(&mut peer_stream).await.unwrap();

        // TODO At this point again, slave tries to connect to other nodes as peer in the cluster
        self.send(ClusterCommand::AddPeer {
            peer_addr,
            stream: peer_stream.0,
            peer_kind: PeerKind::accepted_peer_kind(&repl_info.master_replid, &master_repl_id),
        })
        .await
        .unwrap();
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

    pub(crate) async fn discover_cluster(&'static self, self_port: u16) -> anyhow::Result<()> {
        let repl_info = self.replication_info().await?;

        let master_bind_addr = repl_info.master_cluster_bind_addr();
        let mut outbound_stream = OutboundStream(TcpStream::connect(&master_bind_addr).await?);

        let connection_info = outbound_stream.establish_connection(self_port).await?;

        self.send(ClusterCommand::AddPeer {
            peer_addr: PeerAddr(master_bind_addr),
            stream: outbound_stream.0,
            peer_kind: PeerKind::connected_peer_kind(
                &repl_info.master_replid,
                &connection_info.repl_id,
            ),
        })
        .await?;

        let existing_peers = self.get_peers().await?;
        for peer in connection_info.peer_list {
            // if peer is not in the cluster, connect to it
            if !existing_peers.contains(&PeerAddr(peer.clone())) {
                let mut peer_stream = OutboundStream(TcpStream::connect(peer).await?);
            }
        }

        //TODO: wait to receive file from master
        Ok(())
    }
}
