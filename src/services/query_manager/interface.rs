use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub trait TRead {
    fn read(
        &mut self,
        buf: &mut BytesMut,
    ) -> impl std::future::Future<Output = Result<usize, std::io::Error>> + Send;
}

pub trait TWriteBuf {
    fn write_buf(
        &mut self,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

impl TRead for tokio::net::TcpStream {
    async fn read(&mut self, buf: &mut BytesMut) -> Result<usize, std::io::Error> {
        self.read_buf(buf).await
    }
}

impl TWriteBuf for tokio::net::TcpStream {
    async fn write_buf(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let stream = self as &mut tokio::net::TcpStream;
        stream.write(buf).await?;
        Ok(())
    }
}
