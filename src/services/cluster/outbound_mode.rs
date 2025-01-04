use crate::services::config::replication::Replication;
use crate::services::stream_manager::interface::TStream;
use crate::services::stream_manager::query_io::QueryIO;
use crate::services::stream_manager::request_controller::replica::replication_request::HandShakeResponse;
use crate::{make_smart_pointer, write_array};
use tokio::net::TcpStream;

// The following is used only when the node is in slave mode
pub(crate) struct OutboundStream(pub(crate) TcpStream);
impl OutboundStream {
    pub(crate) async fn estabilish_handshake(
        &mut self,
        replication: Replication,
        self_port: u16,
    ) -> anyhow::Result<String> {
        self.send_ping().await?;
        self.send_replconf_listening_port(self_port).await?;
        self.send_replconf_capa(&replication).await?;

        #[cfg(integration)]
        println!("[INFO] Three-way handshake completed");

        let (repl_id, _offset) = self.send_psync(&replication).await?;

        Ok(repl_id)
    }

    async fn send_ping(&mut self) -> anyhow::Result<()> {
        self.write(write_array!("PING")).await?;

        let HandShakeResponse::PONG = self.extract_response().await? else {
            let err_msg = "PONG not received";
            eprintln!("{}", err_msg);
            return Err(anyhow::anyhow!(err_msg));
        };

        Ok(())
    }

    async fn send_replconf_listening_port(&mut self, self_port: u16) -> anyhow::Result<()> {
        self.write(write_array!(
            "REPLCONF",
            "listening-port",
            self_port.to_string()
        ))
        .await?;

        let HandShakeResponse::OK = self.extract_response().await? else {
            let err_msg = "Ok expected, but not received";
            eprintln!("{}", err_msg);
            return Err(anyhow::anyhow!(err_msg));
        };

        Ok(())
    }

    async fn send_replconf_capa(&mut self, repl_info: &Replication) -> anyhow::Result<()> {
        self.write(write_array!("REPLCONF", "capa", "psync2"))
            .await?;

        let HandShakeResponse::OK = self.extract_response().await? else {
            let err_msg = "Ok expected, but not received";
            eprintln!("{}", err_msg);
            return Err(anyhow::anyhow!(err_msg));
        };
        Ok(())
    }

    async fn send_psync(&mut self, repl_info: &Replication) -> anyhow::Result<(String, i64)> {
        self.write(write_array!("PSYNC", "?", "-1")).await?;

        let HandShakeResponse::FULLRESYNC { repl_id, offset } = self.extract_response().await?
        else {
            let err_msg = "FULLRESYNC not received";
            eprintln!("{}", err_msg);
            return Err(anyhow::anyhow!(err_msg));
        };

        Ok((repl_id, offset))
    }

    async fn extract_response(&mut self) -> anyhow::Result<HandShakeResponse> {
        let query_io = self.read_value().await?;
        match query_io {
            QueryIO::SimpleString(value_array) => Ok(value_array.try_into()?),

            _ => Err(anyhow::anyhow!("Unexpected command format")),
        }
    }
}

make_smart_pointer!(OutboundStream, TcpStream);
