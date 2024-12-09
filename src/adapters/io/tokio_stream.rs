use std::io::ErrorKind;

use crate::{
    services::query_manager::{
        error::IoError,
        interface::{TGetPeerIp, TRead, TStream, TWrite},
    },
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
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), IoError> {
        let mut stream = self as &mut tokio::net::TcpStream;
        AsyncWriteExt::write_all(&mut stream, buf)
            .await
            .map_err(|e| e.kind().into())
    }
}
impl TGetPeerIp for tokio::net::TcpStream {
    fn get_peer_ip(&self) -> Result<String, IoError> {
        let addr = self.peer_addr().map_err(|error| {
            eprintln!("error = {:?}", error);
            IoError::NotConnected
        })?;
        Ok(addr.ip().to_string())
    }
}

pub struct AppStreamListener(TcpListener);
impl AppStreamListener {
    async fn new(bind_addr: String) -> Self {
        AppStreamListener(TcpListener::bind(bind_addr).await.expect("failed to bind"))
    }
}

impl TSocketListener for AppStreamListener {
    async fn accept(&self) -> std::result::Result<(impl TStream, std::net::SocketAddr), IoError> {
        match self.0.accept().await {
            Ok(val) => Ok((val.0, val.1)),
            Err(err) => Err(err.kind().into()),
        }
    }
}
impl TSocketListenerFactory for AppStreamListener {
    async fn create_listner(bind_addr: String) -> impl TSocketListener {
        AppStreamListener::new(bind_addr).await
    }
}

impl From<ErrorKind> for IoError {
    fn from(value: ErrorKind) -> Self {
        match value {
            ErrorKind::ConnectionRefused => IoError::ConnectionRefused,
            ErrorKind::ConnectionReset => IoError::ConnectionReset,
            ErrorKind::ConnectionAborted => IoError::ConnectionAborted,
            ErrorKind::NotConnected => IoError::NotConnected,
            ErrorKind::BrokenPipe => IoError::BrokenPipe,
            ErrorKind::TimedOut => IoError::TimedOut,
            _ => {
                eprintln!("unknown error: {:?}", value);
                IoError::Unknown
            }
        }
    }
}

#[test]
fn test_socket_to_string() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    //WHEN
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

    //THEN
    assert_eq!(socket.ip().to_string(), "127.0.0.1")
}
