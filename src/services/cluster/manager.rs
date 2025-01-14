use super::actors::actor::{ClusterActor, PeerAddr};
use super::actors::command::{ClusterCommand, PeerKind};
use crate::make_smart_pointer;
use crate::services::cluster::inbound::stream::InboundStream;
use crate::services::cluster::outbound::stream::OutboundStream;
use crate::services::config::replication::Replication;
use crate::services::interface::TStream;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::persist::endec::encoder::encoding_processor::{EncodingProcessor, SaveMeta, SaveTarget};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::time::interval;

#[derive(Clone)]
pub struct ClusterManager(Sender<ClusterCommand>);
make_smart_pointer!(ClusterManager, Sender<ClusterCommand>);

impl ClusterManager {
    pub fn run() -> Self {
        let (actor_handler, cluster_message_listener) = tokio::sync::mpsc::channel(100);
        tokio::spawn(
            ClusterActor::default().handle(actor_handler.clone(), cluster_message_listener),
        );

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

    pub(crate) async fn accept_peer(&self, mut peer_stream: InboundStream, self_repl_id: String, cache_manager: &'static CacheManager) {
        // TODO
        let (peer_addr, repl_id) = peer_stream.recv_threeway_handshake().await.unwrap();

        // TODO Need to decide which point to send file data
        // TODO At this point, slave stream must write master_replid so that other nodes can tell where it belongs
        // TODO Remove this sleep

        self.disseminate_peers(&mut peer_stream).await.unwrap();

        // TODO sync only when master
        Self::sync_replica(&mut peer_stream, cache_manager).await;

        // TODO At this point again, slave tries to connect to other nodes as peer in the cluster
        self.send(ClusterCommand::AddPeer {
            peer_addr,
            stream: peer_stream.0,
            peer_kind: PeerKind::peer_kind(&self_repl_id, &repl_id),
        })
            .await
            .unwrap();
    }

    async fn sync_replica(peer_stream: &mut InboundStream, cache_manager: &CacheManager) {
        let mut processor = EncodingProcessor::with_vec(SaveMeta::new(cache_manager.inboxes.len()));
        processor.add_meta().await.unwrap();

        //TODO Create MPSC Channel
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        cache_manager.route_save(tx);
        while let Some(command) = rx.recv().await {
            match processor.handle_cmd(command).await {
                Ok(should_break) => {
                    if should_break {
                        break;
                    }
                }
                Err(err) => {
                    panic!("error while encoding: {:?}", err);
                }
            }
        }
        let SaveTarget::InMemory(in_memory) = processor.target else {
            panic!("SaveTarget should be InMemory");
        };
        peer_stream.write(QueryIO::File(in_memory)).await.unwrap();
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

    pub async fn discover_cluster(
        &'static self,
        repl_info: Replication,
        self_port: u16,
    ) -> anyhow::Result<()> {
        let master_bind_addr = repl_info.master_cluster_bind_addr();
        let mut outbound_stream = OutboundStream(TcpStream::connect(&master_bind_addr).await?);

        let peer_list = outbound_stream.establish_connection(self_port).await?;

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
