use crate::{
    services::query_manager::interface::{TRead, TWrite},
    TSocketListener, TSocketListenerFactory,
};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

impl TRead for tokio::net::TcpStream {
    // TCP doesn't inherently delimit messages.
    // The data arrives in a continuous stream of bytes. And
    // we might not receive all the data in one go.
    // So, we need to read the data in chunks until we have received all the data for the message.
    async fn read_bytes(&mut self, buffer: &mut BytesMut) -> Result<(), std::io::Error> {
        // fixed size buffer
        let mut temp_buffer = [0u8; 512];
        loop {
            let bytes_read = self.read(&mut temp_buffer).await?;

            // If no bytes are read, it suggests that the no more data will be received for this message.
            if bytes_read == 0 {
                break;
            }

            // Extend the buffer with the newly read data
            buffer.extend_from_slice(&temp_buffer[..bytes_read]);

            // If fewer bytes than the buffer size are read, it suggests that
            // - The sender has sent all the data currently available for this message.
            // - You have reached the end of the message.
            if bytes_read < temp_buffer.len() {
                break;
            }
        }
        Ok(())
    }
}

impl TWrite for tokio::net::TcpStream {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let mut stream = self as &mut tokio::net::TcpStream;
        AsyncWriteExt::write_all(&mut stream, buf).await
    }
}

pub struct AppStreamListener(TcpListener);
impl AppStreamListener {
    async fn new(bind_addr: String) -> Self {
        AppStreamListener(TcpListener::bind(bind_addr).await.unwrap())
    }
}

impl TSocketListener for AppStreamListener {
    async fn accept(&self) -> anyhow::Result<(impl TWrite + TRead, std::net::SocketAddr)> {
        self.0.accept().await.map_err(Into::into)
    }
}
impl TSocketListenerFactory for AppStreamListener {
    async fn create_listner(bind_addr: String) -> impl TSocketListener {
        AppStreamListener::new(bind_addr).await
    }
}
