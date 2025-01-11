use crate::services::cluster::outbound::response::ConnectionResponse;
use crate::services::interface::{TRead, TStream};
use crate::services::query_io::QueryIO;
use crate::{make_smart_pointer, write_array};
use tokio::net::TcpStream;

// The following is used only when the node is in slave mode
pub(crate) struct OutboundStream(pub(crate) TcpStream);
impl OutboundStream {
    pub async fn establish_connection(&mut self, self_port: u16) -> anyhow::Result<Vec<String>> {
        // Trigger
        self.write(write_array!("PING")).await?;
        let mut ok_count = 0;

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
                        println!("[INFO] Three-way handshake completed")
                    }
                    ConnectionResponse::PEERS(peer_list) => {
                        println!("[INFO] Received peer list: {:?}", peer_list);
                        return Ok(peer_list);
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
            2 => Ok(write_array!("PSYNC", "?", "-1")),
            _ => Err(anyhow::anyhow!("Unexpected OK count")),
        }
    }
}

make_smart_pointer!(OutboundStream, TcpStream);
