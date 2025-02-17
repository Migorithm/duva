use super::request::{HandShakeRequest, HandShakeRequestEnum};
use crate::make_smart_pointer;
use crate::presentation::cluster_in::create_peer;
use crate::services::cluster::actors::commands::{AddPeer, ClusterCommand};
use crate::services::cluster::peers::identifier::PeerIdentifier;
use crate::services::cluster::peers::kind::PeerKind;
use crate::services::interface::TGetPeerIp;
use crate::services::interface::TRead;
use crate::services::interface::TWrite;
use crate::services::query_io::QueryIO;
use crate::services::statefuls::cache::manager::CacheManager;
use crate::services::statefuls::snapshot::save::actor::SaveTarget;
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;
use anyhow::Context;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

// The following is used only when the node is in master mode
pub(crate) struct InboundStream {
    pub(crate) stream: TcpStream,
    pub(crate) current_offset: u64,
    pub(crate) repl_id: String,
    pub(crate) inbound_peer_addr: Option<PeerIdentifier>,
    pub(crate) inbound_master_replid: Option<String>,
}

make_smart_pointer!(InboundStream, TcpStream => stream);

impl InboundStream {
    pub(crate) fn new(stream: TcpStream, repl_id: String, current_offset: u64) -> Self {
        Self {
            stream,
            inbound_peer_addr: None,
            inbound_master_replid: None,
            current_offset,
            repl_id,
        }
    }
    pub async fn recv_threeway_handshake(&mut self) -> anyhow::Result<()> {
        self.recv_ping().await?;

        let port = self.recv_replconf_listening_port().await?;

        // TODO find use of capa?
        let _capa_val_vec = self.recv_replconf_capa().await?;

        // TODO check repl_id is '?' or of mine. If not, consider incoming as peer
        let (repl_id, _offset) = self.recv_psync().await?;

        self.inbound_peer_addr = Some(format!("{}:{}", self.get_peer_ip()?, port).into());

        self.inbound_master_replid = Some(repl_id);

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

    async fn recv_replconf_capa(&mut self) -> anyhow::Result<Vec<(Bytes, Bytes)>> {
        let mut cmd = self.extract_cmd().await?;
        let capa_val_vec = cmd.extract_capa()?;
        self.write(QueryIO::SimpleString("OK".into())).await?;
        Ok(capa_val_vec)
    }
    async fn recv_psync(&mut self) -> anyhow::Result<(String, i64)> {
        let mut cmd = self.extract_cmd().await?;
        let (repl_id, offset) = cmd.extract_psync()?;

        let (self_master_replid, self_master_repl_offset) =
            (self.repl_id.clone(), self.current_offset);

        self.write(QueryIO::SimpleString(
            format!("FULLRESYNC {} {}", self_master_replid, self_master_repl_offset).into(),
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
        Ok(PeerKind::accepted_peer_kind(
            &self.repl_id,
            self.inbound_master_replid.as_ref().context("No master replid")?,
        ))
    }

    pub(crate) fn to_add_peer(
        self,
        cluster_actor_handler: Sender<ClusterCommand>,
        snapshot_applier: SnapshotApplier,
    ) -> anyhow::Result<ClusterCommand> {
        let kind = self.peer_kind()?;
        let peer_id = self.inbound_peer_addr.context("No peer addr")?;
        let peer = create_peer(
            self.stream,
            kind.clone(),
            peer_id.clone(),
            cluster_actor_handler,
            snapshot_applier,
        );
        Ok(ClusterCommand::AddPeer(AddPeer { peer_id, peer }))
    }

    pub(crate) async fn send_sync_to_inbound_server(
        mut self,
        cache_manager: CacheManager,
    ) -> anyhow::Result<Self> {
        // route save caches
        let task = cache_manager
            .route_save(SaveTarget::InMemory(Vec::new()), self.repl_id.clone(), self.current_offset)
            .await?
            .await??;

        let snapshot = QueryIO::File(task.into_inner().into());
        println!("[INFO] Sent sync to slave {:?}", snapshot);
        self.write(snapshot).await?;

        // collect snapshot data from processor

        Ok(self)
    }
}
