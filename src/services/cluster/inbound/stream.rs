use crate::make_smart_pointer;
use crate::services::cluster::actor::PeerAddr;
use crate::services::cluster::inbound::request::HandShakeRequest;
use crate::services::cluster::inbound::request::HandShakeRequestEnum;

use crate::services::interface::{TGetPeerIp, TStream};
use crate::services::query_io::QueryIO;
use tokio::net::TcpStream;

// The following is used only when the node is in master mode
pub(crate) struct InboundStream(pub TcpStream);

make_smart_pointer!(InboundStream, TcpStream);

impl InboundStream {
    pub async fn recv_threeway_handshake(&mut self) -> anyhow::Result<(PeerAddr, String)> {
        self.recv_ping().await?;

        let port = self.recv_replconf_listening_port().await?;

        // TODO find use of capa?
        let _capa_val_vec = self.recv_replconf_capa().await?;

        // TODO check repl_id is '?' or of mine. If not, consider incoming as peer
        let (_repl_id, _offset) = self.recv_psync().await?;

        Ok((PeerAddr(format!("{}:{}", self.get_peer_ip()?, port)), _repl_id))
    }

    async fn recv_ping(&mut self) -> anyhow::Result<()> {
        let cmd = self.extract_cmd().await?;
        cmd.match_query(HandShakeRequestEnum::Ping)?;

        self.write(QueryIO::SimpleString("PONG".to_string())).await?;
        Ok(())
    }

    async fn recv_replconf_listening_port(&mut self) -> anyhow::Result<u16> {
        let mut cmd = self.extract_cmd().await?;
        let port = cmd.extract_listening_port()?;
        self.write(QueryIO::SimpleString("OK".to_string())).await?;
        Ok(port)
    }

    async fn recv_replconf_capa(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        let mut cmd = self.extract_cmd().await?;
        let capa_val_vec = cmd.extract_capa()?;
        self.write(QueryIO::SimpleString("OK".to_string())).await?;
        Ok(capa_val_vec)
    }
    async fn recv_psync(&mut self) -> anyhow::Result<(String, i64)> {
        let mut cmd = self.extract_cmd().await?;
        let (repl_id, offset) = cmd.extract_psync()?;

        self.write(QueryIO::SimpleString(
            "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
        ))
            .await?;

        Ok((repl_id, offset))
    }

    async fn extract_cmd(&mut self) -> anyhow::Result<HandShakeRequest> {
        let query_io = self.read_value().await?;
        HandShakeRequest::new(query_io)
    }
}
