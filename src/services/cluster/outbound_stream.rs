use crate::services::interface::{TRead, TStream};
use crate::services::query_io::QueryIO;
use crate::{make_smart_pointer, write_array};
use tokio::net::TcpStream;

use super::establishment::outbound::ConnectionResponse;

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
                        self.write(write_array!(
                            "REPLCONF",
                            "listening-port",
                            self_port.to_string()
                        ))
                        .await?
                    }
                    ConnectionResponse::OK => {
                        ok_count += 1;
                        if ok_count == 1 {
                            //send_replconf_capa
                            self.write(write_array!("REPLCONF", "capa", "psync2"))
                                .await?
                        } else if ok_count == 2 {
                            //send_psync
                            self.write(write_array!("PSYNC", "?", "-1")).await?;
                        } else {
                            return Err(anyhow::anyhow!("Unexpected OK count"));
                        }
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
}

make_smart_pointer!(OutboundStream, TcpStream);
