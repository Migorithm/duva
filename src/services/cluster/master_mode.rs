use tokio::{net::TcpStream, task::yield_now};

use crate::{
    make_smart_pointer,
    services::stream_manager::{
        interface::{TExtractQuery, TGetPeerIp, TStream},
        query_io::QueryIO,
        request_controller::replica::{
            arguments::PeerRequestArguments, replication_request::HandShakeRequest,
        },
    },
};

use super::actor::PeerAddr;

pub(crate) struct MasterStream(pub TcpStream)
where
    TcpStream: TExtractQuery<HandShakeRequest, PeerRequestArguments> + TStream;

make_smart_pointer!(MasterStream, TcpStream);

impl MasterStream {
    pub async fn establish_threeway_handshake(&mut self) -> anyhow::Result<(PeerAddr, bool)> {
        self.handle_ping().await?;

        let port = self.handle_replconf_listening_port().await?;

        // TODO find use of capa?
        let _capa_val_vec = self.handle_replconf_capa().await?;

        // TODO check repl_id is '?' or of mine. If not, consider incoming as peer
        let (_repl_id, _offset) = self.handle_psync().await?;

        // ! TODO: STRANGE BEHAVIOUR
        // if not for the following, message is sent with the previosly sent message
        // even with this, it shows flaking behaviour
        yield_now().await;

        Ok((
            PeerAddr(format!("{}:{}", self.get_peer_ip()?, port)),
            _repl_id == "?", // if repl_id is '?' or of mine, it's slave, otherwise it's a peer.
        ))
    }

    async fn handle_ping(&mut self) -> anyhow::Result<()> {
        let (HandShakeRequest::Ping, _) = <tokio::net::TcpStream as TExtractQuery<
            HandShakeRequest,
            PeerRequestArguments,
        >>::extract_query(self)
        .await?
        else {
            return Err(anyhow::anyhow!("Ping not given during handshake"));
        };
        self.write(QueryIO::SimpleString("PONG".to_string()))
            .await?;
        Ok(())
    }

    async fn handle_replconf_listening_port(&mut self) -> anyhow::Result<u16> {
        let (HandShakeRequest::ReplConf, query_args) = <tokio::net::TcpStream as TExtractQuery<
            HandShakeRequest,
            PeerRequestArguments,
        >>::extract_query(self)
        .await?
        else {
            return Err(anyhow::anyhow!("ReplConf not given during handshake"));
        };
        let port = if query_args.first() == Some(&QueryIO::BulkString("listening-port".to_string()))
        {
            query_args.take_replica_port()?
        } else {
            return Err(anyhow::anyhow!("Invalid listening-port given"));
        };
        self.write(QueryIO::SimpleString("OK".to_string())).await?;

        Ok(port.parse::<u16>()?)
    }

    async fn handle_replconf_capa(&mut self) -> anyhow::Result<Vec<(String, String)>> {
        let (HandShakeRequest::ReplConf, query_args) = <tokio::net::TcpStream as TExtractQuery<
            HandShakeRequest,
            PeerRequestArguments,
        >>::extract_query(self)
        .await?
        else {
            return Err(anyhow::anyhow!("ReplConf not given during handshake"));
        };
        let capa_val_vec = query_args.take_capabilities()?;
        self.write(QueryIO::SimpleString("OK".to_string())).await?;

        Ok(capa_val_vec)
    }
    async fn handle_psync(&mut self) -> anyhow::Result<(String, i64)> {
        let (HandShakeRequest::Psync, query_args) = <tokio::net::TcpStream as TExtractQuery<
            HandShakeRequest,
            PeerRequestArguments,
        >>::extract_query(self)
        .await?
        else {
            return Err(anyhow::anyhow!("Psync not given during handshake"));
        };
        let (repl_id, offset) = query_args.take_psync()?;

        self.write(QueryIO::SimpleString(
            "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
        ))
        .await?;

        Ok((repl_id, offset))
    }
}
