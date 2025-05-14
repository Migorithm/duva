use super::request::HandShakeRequest;
use super::request::HandShakeRequestEnum;
use crate::ClusterCommand;
use crate::domains::IoError;

use crate::domains::cluster_actors::listener::PeerListener;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::peers::connected_peer_info::ConnectedPeerInfo;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::PeerState;
use crate::domains::query_parsers::QueryIO;
use crate::services::interface::TRead;
use crate::services::interface::TWrite;
use anyhow::Context;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

// The following is used only when the node is in leader mode
#[derive(Debug)]
pub(crate) struct InboundStream {
    pub(crate) r: OwnedReadHalf,
    pub(crate) w: OwnedWriteHalf,
    pub(crate) self_repl_info: ReplicationState,
    pub(crate) connected_peer_info: Option<ConnectedPeerInfo>,
}

impl InboundStream {
    pub(crate) fn new(stream: TcpStream, self_repl_info: ReplicationState) -> Self {
        let (read, write) = stream.into_split();
        Self { r: read, w: write, self_repl_info, connected_peer_info: None }
    }
    async fn recv_handshake(&mut self) -> anyhow::Result<()> {
        self.recv_ping().await?;

        let port = self.recv_replconf_listening_port().await?;

        // TODO find use of capa?
        let _capa_val_vec = self.recv_replconf_capa().await?;

        let (peer_leader_repl_id, peer_hwm) = self.recv_psync().await?;

        let addr = self.r.peer_addr().map_err(|error| Into::<IoError>::into(error.kind()))?;

        self.connected_peer_info = Some(ConnectedPeerInfo {
            id: PeerIdentifier::new(&addr.ip().to_string(), port),
            replid: peer_leader_repl_id,
            hwm: peer_hwm,
            peer_list: vec![],
        });

        Ok(())
    }

    pub(crate) fn id(&self) -> anyhow::Result<PeerIdentifier> {
        Ok(self.connected_peer_info.as_ref().context("set by now")?.id.clone())
    }
    pub(crate) fn peer_state(&self) -> anyhow::Result<PeerState> {
        Ok(self
            .connected_peer_info
            .as_ref()
            .context("set by now")?
            .decide_peer_kind(&self.self_repl_info.replid))
    }

    async fn recv_ping(&mut self) -> anyhow::Result<()> {
        let cmd = self.extract_cmd().await?;
        cmd.match_query(HandShakeRequestEnum::Ping)?;

        self.w.write(QueryIO::SimpleString("PONG".into())).await?;
        Ok(())
    }

    async fn recv_replconf_listening_port(&mut self) -> anyhow::Result<u16> {
        let mut cmd = self.extract_cmd().await?;

        let port = cmd.extract_listening_port()?;

        self.w.write(QueryIO::SimpleString("OK".into())).await?;

        Ok(port)
    }

    async fn recv_replconf_capa(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        let mut cmd = self.extract_cmd().await?;
        let capa_val_vec = cmd.extract_capa()?;
        self.w.write(QueryIO::SimpleString("OK".into())).await?;
        Ok(capa_val_vec)
    }
    async fn recv_psync(&mut self) -> anyhow::Result<(ReplicationId, u64)> {
        let mut cmd = self.extract_cmd().await?;
        let (inbound_repl_id, offset) = cmd.extract_psync()?;

        // ! Assumption, if self replid is not set at this point but still receives inbound stream, this is leader.

        let (id, self_leader_replid, self_leader_repl_offset) = (
            self.self_repl_info.self_identifier(),
            self.self_repl_info.replid.clone(),
            self.self_repl_info.hwm.load(Ordering::Relaxed),
        );

        self.w
            .write(QueryIO::SimpleString(format!(
                "FULLRESYNC {} {} {}",
                id, self_leader_replid, self_leader_repl_offset
            )))
            .await?;
        self.recv_ok().await?;
        Ok((inbound_repl_id, offset))
    }

    async fn extract_cmd(&mut self) -> anyhow::Result<HandShakeRequest> {
        let mut query_io = self.r.read_values().await?;
        HandShakeRequest::new(query_io.swap_remove(0))
    }

    async fn disseminate_peers(&mut self, peers: Vec<PeerIdentifier>) -> anyhow::Result<()> {
        self.w
            .write(QueryIO::SimpleString(format!(
                "PEERS {}",
                peers.into_iter().map(|x| x.0).collect::<Vec<String>>().join(" ")
            )))
            .await?;

        self.recv_ok().await?;
        Ok(())
    }

    async fn recv_ok(&mut self) -> anyhow::Result<()> {
        let mut query_io = self.r.read_values().await?;
        let Some(query) = query_io.pop() else {
            return Err(anyhow::anyhow!("No query found"));
        };
        let QueryIO::SimpleString(val) = query else {
            return Err(anyhow::anyhow!("Invalid query"));
        };
        if val.to_lowercase() != "ok" {
            return Err(anyhow::anyhow!("Invalid response"));
        }
        Ok(())
    }

    pub(crate) async fn add_peer(
        mut self,
        members: Vec<PeerIdentifier>,
        cluster_handler: tokio::sync::mpsc::Sender<ClusterCommand>,
    ) -> anyhow::Result<()> {
        self.recv_handshake().await?;
        self.disseminate_peers(members).await?;
        let peer = PeerListener::spawn_from_inbound_stream(self, cluster_handler.clone()).await?;
        let _ = cluster_handler.send(ClusterCommand::AddPeer(peer)).await;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::domains::peers::peer::NodeKind;
    use tokio::net::TcpListener;

    use super::*;
    #[tokio::test]
    async fn test_reconnection_on_gossip() {
        use crate::domains::cluster_actors::actor::test::cluster_actor_create_helper;
        // GIVEN
        let mut cluster_actor = cluster_actor_create_helper().await;

        // * run listener to see if connection is attempted
        let listener = TcpListener::bind("127.0.0.1:44455").await.unwrap(); // ! Beaware that this is cluster port
        let bind_addr = listener.local_addr().unwrap();

        let mut replication_state = cluster_actor.replication.clone();
        replication_state.is_leader_mode = false;

        let (tx, rx) = tokio::sync::oneshot::channel();

        // Spawn the listener task
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut inbound_stream = InboundStream::new(stream, replication_state.clone());
            if inbound_stream.recv_handshake().await.is_ok() {
                let _ = tx.send(());
            };
        });

        // WHEN - try to reconnect
        cluster_actor
            .join_peer_network_if_absent(vec![PeerState::new(
                &format!("127.0.0.1:{}", bind_addr.port() - 10000),
                0,
                cluster_actor.replication.replid.clone(),
                NodeKind::Replica,
            )])
            .await;

        assert!(handle.await.is_ok());
        assert!(rx.await.is_ok());
    }
}
