use crate::domains::peers::command::*;
use crate::domains::peers::connections::connection_types::{ReadConnection, WriteConnected};
use crate::domains::query_io::SERDE_CONFIG;
use crate::domains::{IoError, TAsyncReadWrite, TSerdeDynamicWrite, TSerdeWrite};
use std::fmt::Debug;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeWrite for T {
    async fn serialized_write(&mut self, buf: impl bincode::Encode + Send) -> Result<(), IoError> {
        let encoded = bincode::encode_to_vec(buf, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;

        let len = encoded.len() as u32;
        self.write_u32(len).await.map_err(|e| e.kind())?;

        self.write_all(&encoded).await.map_err(|e| e.kind())?;
        Ok(self.flush().await.map_err(|e| e.kind())?)
    }
}

#[async_trait::async_trait]
impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeDynamicWrite
    for T
{
    async fn send(&mut self, msg: PeerMessage) -> Result<(), IoError> {
        let encoded = bincode::encode_to_vec(msg, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;

        let len = encoded.len() as u32;
        self.write_u32(len).await.map_err(|e| e.kind())?;

        self.write_all(&encoded).await.map_err(|e| e.kind())?;
        Ok(self.flush().await.map_err(|e| e.kind())?)
    }

    async fn send_connection_msg(&mut self, arg: &str) -> Result<(), IoError> {
        self.serialized_write(arg).await
    }
}

impl TAsyncReadWrite for TcpStream {
    async fn connect_and_split(
        connect_to: &str,
    ) -> Result<(ReadConnection<OwnedReadHalf>, WriteConnected), IoError> {
        let stream = TcpStream::connect(connect_to).await.map_err(|e| e.kind())?;
        let (r, w) = stream.into_split();
        Ok((ReadConnection::new(r), WriteConnected(Box::new(w))))
    }
}
