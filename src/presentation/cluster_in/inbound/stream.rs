use super::request::HandShakeRequest;
use super::request::HandShakeRequestEnum;
use crate::domains::append_only_files::WriteOperation;
use crate::domains::cluster_actors::commands::AddPeer;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::replication::ReplicationInfo;
use crate::domains::peers::connected_peer_info::ConnectedPeerInfo;
use crate::domains::peers::identifier::PeerIdentifier;
use crate::domains::peers::kind::PeerKind;
use crate::domains::peers::peer::Peer;
use crate::domains::query_parsers::QueryIO;
use crate::make_smart_pointer;
use crate::presentation::cluster_in::communication_manager::ClusterCommunicationManager;
use crate::services::interface::TGetPeerIp;
use crate::services::interface::TRead;
use crate::services::interface::TWrite;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

// The following is used only when the node is in leader mode
pub(crate) struct InboundStream {
    pub(crate) stream: TcpStream,
    pub(crate) self_repl_info: ReplicationInfo,
    pub(crate) peer_info: ConnectedPeerInfo,
}

make_smart_pointer!(InboundStream, TcpStream => stream);

impl InboundStream {
    pub(crate) fn new(stream: TcpStream, self_repl_info: ReplicationInfo) -> Self {
        Self { stream, self_repl_info, peer_info: Default::default() }
    }
    pub async fn recv_threeway_handshake(&mut self) -> anyhow::Result<()> {
        self.recv_ping().await?;

        let port = self.recv_replconf_listening_port().await?;

        // TODO find use of capa?
        let _capa_val_vec = self.recv_replconf_capa().await?;

        // TODO check repl_id is '?' or of mine. If not, consider incoming as peer
        let (peer_leader_repl_id, peer_hwm) = self.recv_psync().await?;

        self.peer_info.id = PeerIdentifier::new(&self.get_peer_ip()?, port);
        self.peer_info.leader_repl_id = peer_leader_repl_id.into();
        self.peer_info.hwm = peer_hwm;

        Ok(())
    }

    async fn recv_ping(&mut self) -> anyhow::Result<()> {
        let cmd = self.extract_cmd().await?;
        cmd.match_query(HandShakeRequestEnum::Ping)?;

        self.write(QueryIO::SimpleString("PONG".into())).await?;
        Ok(())
    }

    async fn recv_replconf_listening_port(&mut self) -> anyhow::Result<u16> {
        let mut cmd = self.extract_cmd().await?;

        let port = cmd.extract_listening_port()?;

        self.write(QueryIO::SimpleString("OK".into())).await?;

        Ok(port)
    }

    async fn recv_replconf_capa(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        let mut cmd = self.extract_cmd().await?;
        let capa_val_vec = cmd.extract_capa()?;
        self.write(QueryIO::SimpleString("OK".into())).await?;
        Ok(capa_val_vec)
    }
    async fn recv_psync(&mut self) -> anyhow::Result<(String, u64)> {
        let mut cmd = self.extract_cmd().await?;
        let (repl_id, offset) = cmd.extract_psync()?;

        let (id, self_leader_replid, self_leader_repl_offset) = (
            self.self_repl_info.self_identifier(),
            self.self_repl_info.leader_repl_id.clone(),
            self.self_repl_info.hwm,
        );

        self.write(QueryIO::SimpleString(
            format!("FULLRESYNC {} {} {}", id, self_leader_replid, self_leader_repl_offset).into(),
        ))
        .await?;

        Ok((repl_id, offset))
    }

    async fn extract_cmd(&mut self) -> anyhow::Result<HandShakeRequest> {
        let mut query_io = self.read_values().await?;
        HandShakeRequest::new(query_io.swap_remove(0))
    }

    pub(crate) async fn disseminate_peers(
        &mut self,
        peers: Vec<PeerIdentifier>,
    ) -> anyhow::Result<()> {
        self.write(QueryIO::SimpleString(
            format!("PEERS {}", peers.into_iter().map(|x| x.0).collect::<Vec<String>>().join(" "))
                .into(),
        ))
        .await?;
        Ok(())
    }

    pub(crate) fn peer_kind(&self) -> anyhow::Result<PeerKind> {
        Ok(PeerKind::decide_peer_kind(&self.self_repl_info.leader_repl_id, self.peer_info.clone()))
    }

    pub(crate) fn to_add_peer(
        self,
        cluster_actor_handler: Sender<ClusterCommand>,
    ) -> anyhow::Result<ClusterCommand> {
        let kind = self.peer_kind()?;

        let peer = Peer::create(self.stream, kind.clone(), cluster_actor_handler);
        Ok(ClusterCommand::AddPeer(AddPeer { peer_id: self.peer_info.id, peer }))
    }

    // depending on the condition, try full/partial sync.
    pub(crate) async fn may_try_sync(
        &mut self,
        ccm: ClusterCommunicationManager,
    ) -> anyhow::Result<()> {
        if let PeerKind::Follower { watermark, leader_repl_id } = self.peer_kind()? {
            if self.self_repl_info.self_identifier() == leader_repl_id {
                let logs = ccm.fetch_logs_for_sync().await?;
                self.write_io(logs).await?;
            }
        }
        Ok(())
    }
}
