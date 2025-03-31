//! A **non-production** client utility for simluating client connections.
//!
//! The `ClientStreamHandler` provides convenience methods for sending data and reading responses.
//!
//! Note: This utility is intended for **testing** and **development** only.
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use uuid::Uuid;

use crate::services::interface::TSerdeReadWrite;

use super::authentications::{AuthRequest, AuthResponse};
/// A client utility for reading and writing asynchronously over a TCP stream.
pub struct ClientStreamHandler {
    /// The owned read-half of the TCP stream.
    pub read_half: OwnedReadHalf,
    /// The owned write-half of the TCP stream.
    pub write_half: OwnedWriteHalf,
    pub client_id: Uuid,
}

impl ClientStreamHandler {
    /// Creates a new `ClientStreamHandler` by connecting to the specified `bind_addr`.
    ///
    /// # Panics
    /// Panics if the connection to `bind_addr` fails.
    pub async fn new(bind_addr: String) -> Self {
        while tokio::net::TcpStream::connect(&bind_addr).await.is_err() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        let mut stream = tokio::net::TcpStream::connect(bind_addr).await.unwrap();

        stream.serialized_write(AuthRequest::ConnectWithoutId).await.unwrap(); // client_id not exist

        let AuthResponse { client_id, request_id } = stream.deserialized_read().await.unwrap();
        let client_id = Uuid::parse_str(&client_id).unwrap();

        let (read_half, write_half) = stream.into_split();
        Self { read_half, write_half, client_id }
    }

    /// Sends a byte slice to the server.
    ///
    /// After writing the bytes, it flushes the output to ensure the data is actually sent.
    pub async fn send(&mut self, operation: &[u8]) {
        self.write_half.write_all(operation).await.unwrap();
        self.write_half.flush().await.unwrap();
    }

    /// Reads a response from the server.
    pub async fn get_response(&mut self) -> String {
        let mut response = Vec::<u8>::new();
        let mut buf = [0u8; 1024];

        // Keep reading in a loop until we receive the full response.
        loop {
            // Attempt to read from the stream.
            let bytes_read = self.read_half.read(&mut buf).await.unwrap();

            if bytes_read == 0 {
                // The connection closed or EOF reached.
                break;
            }

            response.extend_from_slice(&buf[..bytes_read]);

            // If fewer bytes than the buffer size are read, it suggest that
            // - The sender has sent all the data currently available for this message.
            // - You have reached the end of the message.
            if bytes_read < buf.len() {
                break;
            }
        }

        String::from_utf8_lossy(&response).into_owned()
    }

    pub async fn send_and_get(&mut self, operation: &[u8]) -> String {
        self.send(operation).await;
        self.get_response().await
    }
}
