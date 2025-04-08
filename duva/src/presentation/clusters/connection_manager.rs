use std::collections::VecDeque;

use tokio::sync::mpsc::Sender;

use super::communication_manager::ClusterCommunicationManager;
use super::outbound::stream::OutboundStream;

use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::{InboundStream, make_smart_pointer};

pub(crate) struct ClusterConnectionManager(pub(crate) ClusterCommunicationManager);

impl ClusterConnectionManager {
    pub(crate) async fn accept_inbound_stream(
        &self,
        mut peer_stream: InboundStream,
        ccm: ClusterCommunicationManager,
    ) -> anyhow::Result<()> {
        let connected_peer_info = peer_stream.recv_threeway_handshake().await?;

        peer_stream.disseminate_peers(self.0.get_peers().await?).await?;
        peer_stream.may_try_sync(ccm, &connected_peer_info).await?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send(peer_stream.into_add_peer(self.handler(), connected_peer_info, tx)?).await?;

        Ok(())
    }

    pub(crate) async fn discover_cluster(
        self,
        self_port: u16,
        connect_to: PeerIdentifier,
    ) -> anyhow::Result<()> {
        let mut queue = VecDeque::from(vec![connect_to.clone()]);
        let mut callbacks = Vec::new();
        while let Some(connect_to) = queue.pop_front() {
            let (tx, callback) = tokio::sync::oneshot::channel();
            queue.extend(
                ClusterConnectionManager(self.clone())
                    .connect_to_server(self_port, connect_to, tx)
                    .await?,
            );
            callbacks.push(callback);
        }
        for callback in callbacks {
            let _ = callback.await;
        }
        Ok(())
    }

    async fn connect_to_server(
        self,
        self_port: u16,
        connect_to: PeerIdentifier,
        callback: tokio::sync::oneshot::Sender<()>,
    ) -> anyhow::Result<Vec<PeerIdentifier>> {
        let existing_peers = self.get_peers().await?;
        if existing_peers.contains(&connect_to) {
            return Ok(vec![]);
        }

        let replication_info = self.replication_info().await?;
        let (add_peer_cmd, peer_list) = OutboundStream::new(connect_to, replication_info)
            .await?
            .establish_connection(self_port)
            .await?
            .set_replication_info(&self)
            .await?
            .create_peer_cmd(self.handler(), callback)?;
        self.send(add_peer_cmd).await?;

        Ok(peer_list)
    }

    pub async fn send(&self, cmd: ClusterCommand) -> anyhow::Result<()> {
        Ok(self.0.send(cmd).await?)
    }

    fn handler(&self) -> Sender<ClusterCommand> {
        self.0.0.clone()
    }
}

make_smart_pointer!(ClusterConnectionManager, ClusterCommunicationManager);
