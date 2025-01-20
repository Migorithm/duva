use crate::services::cluster::actors::types::PeerAddr;
use crate::services::cluster::outbound::response::ConnectionResponse;
use crate::services::interface::{TRead, TStream};
use crate::services::query_io::QueryIO;
use crate::{from_to, make_smart_pointer, write_array};
use tokio::net::TcpStream;

// The following is used only when the node is in slave mode
pub(crate) struct OutboundStream(pub(crate) TcpStream);

impl OutboundStream {
    pub(crate) async fn new(connect_to: &str) -> anyhow::Result<Self> {
        Ok(OutboundStream(TcpStream::connect(connect_to).await?))
    }
    pub async fn establish_connection(&mut self, self_port: u16) -> anyhow::Result<ConnectionInfo> {
        // Trigger
        self.write(write_array!("PING")).await?;
        let mut ok_count = 0;
        let mut connection_info = ConnectionInfo::default();

        loop {
            let res = self.read_values().await?;
            for query in res {
                match ConnectionResponse::try_from(query)? {
                    ConnectionResponse::PONG => {
                        let msg = self.after_pong(self_port);
                        self.write(msg).await?
                    }
                    ConnectionResponse::OK => {
                        ok_count += 1;
                        let msg = self.after_ok(ok_count)?;
                        self.write(msg).await?
                    }
                    ConnectionResponse::FULLRESYNC { repl_id, offset } => {
                        connection_info.repl_id = repl_id;
                        connection_info.offset = offset;
                        println!("[INFO] Three-way handshake completed")
                    }
                    ConnectionResponse::PEERS(peer_list) => {
                        println!("[INFO] Received peer list: {:?}", peer_list);
                        connection_info.peer_list = peer_list;
                        return Ok(connection_info);
                    }
                }
            }
        }
    }

    fn after_pong(&self, self_port: u16) -> QueryIO {
        write_array!("REPLCONF", "listening-port", self_port.to_string())
    }
    fn after_ok(&self, ok_count: i32) -> anyhow::Result<QueryIO> {
        match ok_count {
            1 => Ok(write_array!("REPLCONF", "capa", "psync2")),
            2 => Ok(write_array!("PSYNC", "?", "-1")), // TODO "?" here means the server is undecided about their master.
            _ => Err(anyhow::anyhow!("Unexpected OK count")),
        }
    }
}

make_smart_pointer!(OutboundStream, TcpStream);
from_to!(TcpStream, OutboundStream);

#[derive(Debug, Default)]
pub(crate) struct ConnectionInfo {
    // TODO repl_id here is the master_replid from connected server.
    // TODO Set repl_id if given server's repl_id is "?" otherwise, it means that now it's connected to peer.
    pub(crate) repl_id: String,
    pub(crate) offset: i64,
    pub(crate) peer_list: Vec<String>,
}

impl ConnectionInfo {
    pub(crate) fn list_peer_binding_addrs(&self) -> Vec<PeerAddr> {
        self.peer_list
            .iter()
            .flat_map(|peer| {
                if let Some((ip, port)) = peer.rsplit_once(':') {
                    Some(
                        (ip.to_string()
                            + ":"
                            + (port.parse::<u16>().unwrap() + 10000).to_string().as_str())
                        .into(),
                    )
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}
