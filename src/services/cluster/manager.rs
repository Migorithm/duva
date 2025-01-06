use crate::make_smart_pointer;
use crate::services::cluster::actor::{ClusterActor, PeerAddr};
use crate::services::cluster::command::ClusterCommand;
use crate::services::config::replication::Replication;
use crate::services::interface::TStream;
use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

use super::inbound_stream::InboundStream;
use super::outbound_stream::OutboundStream;

#[derive(Clone)]
pub struct ClusterManager(Sender<ClusterCommand>);
make_smart_pointer!(ClusterManager, Sender<ClusterCommand>);

impl ClusterManager {
    pub fn run(actor: ClusterActor) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(actor.handle(rx));
        Self(tx)
    }

    pub(crate) async fn get_peers(&self) -> anyhow::Result<Vec<PeerAddr>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(ClusterCommand::GetPeers(tx)).await?;
        let peers = rx.await?;
        Ok(peers)
    }

    pub(crate) async fn accept_peer(&self, mut peer_stream: InboundStream) {
        let (peer_addr, is_slave) = peer_stream.recv_threeway_handshake().await.unwrap();

        // TODO At this point, slave stream must write master_replid so that other nodes can tell where it belongs
        self.disseminate_peers(&mut peer_stream).await.unwrap();

        // TODO At this point again, slave tries to connect to other nodes as peer in the cluster
        self.send(ClusterCommand::AddPeer {
            peer_addr,
            stream: peer_stream.0,
            is_slave,
        })
        .await
        .unwrap();
    }

    async fn disseminate_peers(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let peers = self.get_peers().await?;
        if peers.is_empty() {
            return Ok(());
        }

        stream
            .write(QueryIO::SimpleString(format!(
                "PEERS {}",
                peers
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            )))
            .await?;
        Ok(())
    }

    pub async fn join_master(
        &'static self,
        repl_info: Replication,
        self_port: u16,
    ) -> anyhow::Result<()> {
        let mut outbound_stream =
            OutboundStream(TcpStream::connect(repl_info.master_cluster_bind_addr()).await?);
        outbound_stream
            .estabilish_handshake(repl_info, self_port)
            .await?;

        Ok(())
    }
}
