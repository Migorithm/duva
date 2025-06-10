use super::request::HandShakeRequest;
use super::request::HandShakeRequestEnum;
use crate::domains::IoError;
use crate::domains::QueryIO;
use crate::domains::cluster_actors::ConnectionMessage;
use crate::domains::cluster_actors::actor::ClusterCommandHandler;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::cluster_actors::replication::ReplicationState;
use crate::domains::interface::TRead;
use crate::domains::interface::TWrite;
use crate::domains::peers::connections::connection_types::ConnectedPeerInfo;
use crate::domains::peers::connections::connection_types::WriteConnected;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::peer::Peer;
use crate::domains::peers::peer::PeerState;
use crate::domains::peers::service::PeerListener;
use anyhow::Context;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

// The following is used only when the node is in leader mode
#[derive(Debug)]
pub(crate) struct InboundStream {
    r: OwnedReadHalf,
    w: OwnedWriteHalf,
    self_repl_info: ReplicationState,
    connected_peer_info: Option<ConnectedPeerInfo>,
}

impl InboundStream {
    pub(crate) fn new(stream: TcpStream, self_repl_info: ReplicationState) -> Self {
        let (read, write) = stream.into_split();
        Self { r: read, w: write, self_repl_info, connected_peer_info: None }
    }
    pub(crate) async fn recv_handshake(&mut self) -> anyhow::Result<()> {
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
        });

        Ok(())
    }

    pub(crate) fn connected_peer_state(&self) -> anyhow::Result<PeerState> {
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
        cluster_handler: ClusterCommandHandler,
    ) -> anyhow::Result<()> {
        self.recv_handshake().await?;

        let peer_state = self.connected_peer_state()?;
        let kill_switch =
            PeerListener::spawn(self.r, cluster_handler.clone(), peer_state.addr.clone());
        let peer = Peer::new(WriteConnected(Box::new(self.w)), peer_state, kill_switch);
        let _ = cluster_handler.send(ConnectionMessage::AddPeer(peer, None)).await;
        Ok(())
    }
}
