use crate::make_smart_pointer;
use crate::services::cluster::actor::{ClusterActor, PeerAddr};
use crate::services::cluster::command::{ClusterCommand, PeerKind};
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

    pub(crate) async fn accept_peer(&self, mut peer_stream: InboundStream, self_repl_id: String) {
        let (peer_addr, repl_id) = peer_stream.recv_threeway_handshake().await.unwrap();

        // TODO Need to decide which point to send file data
        // TODO At this point, slave stream must write master_replid so that other nodes can tell where it belongs
        // TODO Remove this sleep

        self.disseminate_peers(&mut peer_stream).await.unwrap();

        // TODO At this point again, slave tries to connect to other nodes as peer in the cluster
        self.send(ClusterCommand::AddPeer {
            peer_addr,
            stream: peer_stream.0,
            peer_kind: PeerKind::peer_kind(&self_repl_id, &repl_id),
        })
        .await
        .unwrap();
    }

    async fn disseminate_peers(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let peers = self.get_peers().await?;
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

    pub async fn discover_cluster(
        &'static self,
        repl_info: Replication,
        self_port: u16,
    ) -> anyhow::Result<()> {
        let master_bind_addr = repl_info.master_cluster_bind_addr();
        let mut outbound_stream = OutboundStream(TcpStream::connect(&master_bind_addr).await?);

        //TODO: use repl_id and offset
        let (repl_id, offset) = outbound_stream
            .estabilish_handshake(repl_info, self_port)
            .await?;

        let peer_list = outbound_stream.recv_peer_list().await?;

        self.send(ClusterCommand::AddPeer {
            peer_addr: PeerAddr(master_bind_addr),
            stream: outbound_stream.0,
            peer_kind: PeerKind::Master,
        })
        .await?;

        for peer in peer_list {
            let mut peer_stream = OutboundStream(TcpStream::connect(peer).await?);
        }

        //TODO: wait to receive file from master
        Ok(())
    }
}
