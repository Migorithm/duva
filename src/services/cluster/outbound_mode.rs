use crate::{
    make_smart_pointer,
    services::{
        config::replication::Replication,
        stream_manager::{interface::TStream, query_io::QueryIO},
    },
};
use anyhow::Context;
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
        self.send_psync(&replication).await?;
        Ok("".to_string())
    }

    async fn send_ping(&mut self) -> anyhow::Result<()> {
        self.write(QueryIO::Array(vec![QueryIO::BulkString(
            "PING".to_string(),
        )]))
        .await?;

        let QueryIO::SimpleString(val) = self.read_value().await? else {
            return Err(anyhow::anyhow!("PONG not received"));
        };
        assert_eq!(val, "PONG");
        Ok(())
    }

    async fn send_replconf_listening_port(&mut self, self_port: u16) -> anyhow::Result<()> {
        self.write(QueryIO::Array(vec![
            QueryIO::BulkString("REPLCONF".to_string()),
            QueryIO::BulkString("listening-port".to_string()),
            QueryIO::BulkString(self_port.to_string()),
        ]))
        .await?;

        let QueryIO::SimpleString(val) = self.read_value().await? else {
            return Err(anyhow::anyhow!("REPLCONF OK not received"));
        };

        assert_eq!(val, "OK");
        Ok(())
    }

    async fn send_replconf_capa(&mut self, repl_info: &Replication) -> anyhow::Result<()> {
        self.write(QueryIO::Array(vec![
            QueryIO::BulkString("REPLCONF".to_string()),
            QueryIO::BulkString("capa".to_string()),
            QueryIO::BulkString("psync2".to_string()),
        ]))
        .await?;

        let QueryIO::SimpleString(val) = self.read_value().await? else {
            return Err(anyhow::anyhow!("REPLCONF capa not received"));
        };
        assert_eq!(val, "OK");
        Ok(())
    }

    async fn send_psync(&mut self, repl_info: &Replication) -> anyhow::Result<(String, i64)> {
        self.write(QueryIO::Array(vec![
            QueryIO::BulkString("PSYNC".to_string()),
            QueryIO::BulkString("?".to_string()),
            QueryIO::BulkString("-1".to_string()),
        ]))
        .await?;

        let QueryIO::SimpleString(val) = self.read_value().await? else {
            return Err(anyhow::anyhow!("PSYNC not received"));
        };

        let mut iter = val.split_whitespace().skip(1);
        let repl_id = iter
            .next()
            .context("replication_id must be given")?
            .to_string();

        let offset = iter.next().unwrap().parse::<i64>()?;

        assert_eq!(val, format!("FULLRESYNC {} {}", repl_id, offset));
        Ok((repl_id, offset))
    }
}

make_smart_pointer!(OutboundStream, TcpStream);
