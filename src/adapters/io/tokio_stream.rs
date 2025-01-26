use crate::services::error::IoError;
use crate::services::interface::{TGetPeerIp, TRead, TStream, TWrite};
use crate::services::query_io::{deserialize, QueryIO};
use bytes::BytesMut;
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl TStream for tokio::net::TcpStream {
    async fn write(&mut self, value: QueryIO) -> Result<(), IoError> {
        self.write_all(&value.serialize()).await.map_err(|e| Into::<IoError>::into(e.kind()))
    }
}

impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send> TWrite for T {
    async fn write(&mut self, buf: &[u8]) -> Result<(), IoError> {
        self.write_all(buf).await.map_err(|e| Into::<IoError>::into(e.kind()))
    }
}

impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send> TRead for T {
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

    async fn read_values(&mut self) -> anyhow::Result<Vec<QueryIO>> {
        let mut buffer = BytesMut::with_capacity(512);
        self.read_bytes(&mut buffer).await?;

        let mut parsed_values = Vec::new();
        let mut remaining_buffer = buffer;

        while !remaining_buffer.is_empty() {
            match deserialize(remaining_buffer.clone()) {
                Ok((query_io, consumed)) => {
                    parsed_values.push(query_io);

                    // * Remove the parsed portion from the buffer
                    remaining_buffer = remaining_buffer.split_off(consumed);
                }
                Err(e) => {
                    // Handle parsing errors
                    // You might want to log the error or handle it differently based on your use case
                    return Err(anyhow::anyhow!("Parsing error: {:?}", e));
                }
            }
        }
        Ok(parsed_values)
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

#[tokio::test]
async fn test_read_values() {
    let mut buffer = BytesMut::with_capacity(512);
    // add a simple string to buffer
    buffer.extend_from_slice(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n");
    buffer.extend_from_slice(b"+PEERS localhost:6378\r\n");
    // add an integer to buffer

    let mut parsed_values = vec![];
    while !buffer.is_empty() {
        if let Ok((query_io, consumed)) = deserialize(buffer.clone()) {
            parsed_values.push(query_io);

            // * Remove the parsed portion from the buffer
            buffer = buffer.split_off(consumed);
        }
    }

    assert_eq!(parsed_values.len(), 2);
    assert_eq!(
        parsed_values[0],
        QueryIO::SimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string())
    );
    assert_eq!(parsed_values[1], QueryIO::SimpleString("PEERS localhost:6378".to_string()));
}
