use crate::domains::interface::{TRead, TWrite};
use crate::domains::peers::command::*;
use crate::domains::peers::connections::connection_types::{ReadConnected, WriteConnected};
use crate::domains::query_io::SERDE_CONFIG;
use crate::domains::{
    IoError, TAsyncReadWrite, TReadBytes, TSerdeDynamicRead, TSerdeDynamicWrite, TSerdeRead,
    TSerdeWrite,
};
use crate::domains::{QueryIO, deserialize};
use bytes::BytesMut;
use std::fmt::Debug;
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BUFFER_SIZE: usize = 512;
const INITIAL_CAPACITY: usize = 1024;

#[async_trait::async_trait]
impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send + Debug + 'static> TWrite for T {
    async fn write(&mut self, io: QueryIO) -> Result<(), IoError> {
        self.write_all(&io.serialize()).await.map_err(|e| io_error_from_kind(e.kind()))
    }
}

#[async_trait::async_trait]
impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TReadBytes for T {
    // TCP doesn't inherently delimit messages.
    // The data arrives in a continuous stream of bytes. And
    // we might not receive all the data in one go.
    // So, we need to read the data in chunks until we have received all the data for the message.
    async fn read_bytes(&mut self, buffer: &mut BytesMut) -> Result<(), IoError> {
        let mut temp_buffer = [0u8; BUFFER_SIZE];
        loop {
            let bytes_read =
                self.read(&mut temp_buffer).await.map_err(|err| io_error_from_kind(err.kind()))?;

            if bytes_read == 0 {
                // read 0 bytes AND buffer is empty - connection closed
                if buffer.is_empty() {
                    return Err(IoError::ConnectionAborted);
                }
                // read 0 bytes but buffer is not empty - end of message
                return Ok(());
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

#[async_trait::async_trait]
impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TRead for T {
    async fn read_values(&mut self) -> Result<Vec<QueryIO>, IoError> {
        let mut buffer = BytesMut::with_capacity(INITIAL_CAPACITY);
        self.read_bytes(&mut buffer).await?;

        let mut parsed_values = Vec::new();
        let mut remaining_buffer = buffer;

        while !remaining_buffer.is_empty() {
            match deserialize(remaining_buffer.clone()) {
                Ok((query_io, consumed)) => {
                    parsed_values.push(query_io);
                    remaining_buffer = remaining_buffer.split_off(consumed);
                },
                Err(e) => {
                    return Err(IoError::Custom(format!("Parsing error: {e:?}")));
                },
            }
        }
        Ok(parsed_values)
    }
}

#[async_trait::async_trait]
impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeDynamicRead for T {
    async fn receive_peer_msgs(&mut self) -> Result<Vec<PeerMessage>, IoError> {
        let mut buffer = BytesMut::with_capacity(INITIAL_CAPACITY);
        self.read_bytes(&mut buffer).await?;
        let mut parsed_values = Vec::new();
        while !buffer.is_empty() {
            let (request, size) = bincode::decode_from_slice(&buffer, SERDE_CONFIG)
                .map_err(|e| IoError::Custom(e.to_string()))?;
            parsed_values.push(request);
            buffer = buffer.split_off(size);
        }
        Ok(parsed_values)
    }
    async fn receive_connection_msgs(&mut self) -> Result<String, IoError> {
        self.deserialized_read().await
    }
}

impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeWrite for T {
    async fn serialized_write(&mut self, buf: impl bincode::Encode + Send) -> Result<(), IoError> {
        let encoded = bincode::encode_to_vec(buf, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;
        self.write_all(&encoded).await.map_err(|e| io_error_from_kind(e.kind()))
    }
}

#[async_trait::async_trait]
impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeDynamicWrite
    for T
{
    async fn send(&mut self, msg: PeerMessage) -> Result<(), IoError> {
        let encoded = bincode::encode_to_vec(msg, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;
        self.write_all(&encoded).await.map_err(|e| io_error_from_kind(e.kind()))
    }

    async fn send_connection_msg(&mut self, arg: &str) -> Result<(), IoError> {
        self.serialized_write(arg).await
    }
}

impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeRead for T {
    async fn deserialized_read<U>(&mut self) -> Result<U, IoError>
    where
        U: bincode::Decode<()>,
    {
        let mut buffer = BytesMut::with_capacity(INITIAL_CAPACITY);
        self.read_bytes(&mut buffer).await?;

        let (request, _) = bincode::decode_from_slice(&buffer, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;

        Ok(request)
    }

    async fn deserialized_reads<U>(&mut self) -> Result<Vec<U>, IoError>
    where
        U: bincode::Decode<()>,
    {
        let mut buffer = BytesMut::with_capacity(INITIAL_CAPACITY);
        self.read_bytes(&mut buffer).await?;

        let mut parsed_values = Vec::new();

        while !buffer.is_empty() {
            let (request, size) = bincode::decode_from_slice(&buffer, SERDE_CONFIG)
                .map_err(|e| IoError::Custom(e.to_string()))?;
            parsed_values.push(request);
            buffer = buffer.split_off(size);
        }
        Ok(parsed_values)
    }
}

impl TAsyncReadWrite for TcpStream {
    async fn connect(connect_to: &str) -> Result<(ReadConnected, WriteConnected), IoError> {
        let stream =
            TcpStream::connect(connect_to).await.map_err(|e| io_error_from_kind(e.kind()))?;
        let (r, w) = stream.into_split();
        Ok((ReadConnected(Box::new(r)), WriteConnected(Box::new(w))))
    }
}

fn io_error_from_kind(kind: ErrorKind) -> IoError {
    match kind {
        ErrorKind::ConnectionRefused => IoError::ConnectionRefused,
        ErrorKind::ConnectionReset => IoError::ConnectionReset,
        ErrorKind::ConnectionAborted => IoError::ConnectionAborted,
        ErrorKind::NotConnected => IoError::NotConnected,
        ErrorKind::BrokenPipe => IoError::BrokenPipe,
        ErrorKind::TimedOut => IoError::TimedOut,
        _ => {
            eprintln!("unknown error: {kind:?}");
            IoError::Custom(format!("unknown error: {kind:?}"))
        },
    }
}

impl From<ErrorKind> for IoError {
    fn from(value: ErrorKind) -> Self {
        io_error_from_kind(value)
    }
}

#[cfg(test)]
pub mod test_tokio_stream_impl {
    use crate::types::BinBytes;

    use super::*;
    #[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct TestMessage {
        id: u32,
        data: String,
    }

    /// A mock that implements AsyncRead for testing
    #[derive(Debug)]
    struct MockAsyncStream {
        chunks: Vec<Vec<u8>>,
        current_chunk: usize,
    }
    impl MockAsyncStream {
        /// Creates a new mock stream from a vector of byte chunks.
        /// Each inner Vec<u8> represents a single return from the `read` method.
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            MockAsyncStream { chunks, current_chunk: 0 }
        }
    }

    // Must implement AsyncRead for the blanket impl of TRead to work
    impl tokio::io::AsyncRead for MockAsyncStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let self_mut = self.get_mut();

            if self_mut.current_chunk >= self_mut.chunks.len() {
                // All chunks have been read, simulate EOF (read 0 bytes)
                return std::task::Poll::Ready(Ok(()));
            }

            let chunk = &self_mut.chunks[self_mut.current_chunk];
            let bytes_to_copy = std::cmp::min(buf.remaining(), chunk.len());

            // Copy data into the ReadBuf
            buf.put_slice(&chunk[..bytes_to_copy]);

            // Note: Real world scenarios would handle `Poll::Pending` here,
            // but for unit tests, we usually return `Ready` to keep them synchronous.

            self_mut.current_chunk += 1;
            std::task::Poll::Ready(Ok(()))
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
        let mut buffer = BytesMut::with_capacity(INITIAL_CAPACITY);
        // add a simple string to buffer
        let sync_msg = "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0";

        buffer.extend_from_slice(
            format!(
                "${}\r\nFULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n",
                sync_msg.len()
            )
            .as_bytes(),
        );

        let peer_info_msg = "PEERS 127.0.0.1:6378";
        buffer.extend_from_slice(
            format!("${}\r\nPEERS 127.0.0.1:6378\r\n", peer_info_msg.len()).as_bytes(),
        );
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
            QueryIO::BulkString(BinBytes::new(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
            ))
        );

        assert_eq!(parsed_values[1], QueryIO::BulkString(BinBytes::new("PEERS 127.0.0.1:6378")));
    }

    #[tokio::test]
    async fn test_deserialize_reads() {
        // 1. Arrange: Single message in one chunk
        let msg = TestMessage { id: 1, data: "quick".to_string() };

        let encoded = bincode::encode_to_vec(&msg, SERDE_CONFIG).unwrap();
        let encoded_msg = BytesMut::from(encoded.as_slice());
        let mut mock = MockAsyncStream::new(vec![encoded_msg.into()]);

        // 2. Act
        let result: Result<Vec<TestMessage>, IoError> = mock.deserialized_reads().await;

        // 3. Assert
        let deserialized = result.unwrap();
        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0], msg);
    }

    #[tokio::test]
    async fn test_deserialize_reads_vec() {
        // 1. Arrange: Single message in one chunk

        let message_one = TestMessage { id: 1, data: "quick".to_string() };
        let message_two = TestMessage { id: 2, data: "silver".to_string() };
        let mut raw_data = vec![];
        raw_data.extend_from_slice(&bincode::encode_to_vec(message_one, SERDE_CONFIG).unwrap());
        raw_data.extend_from_slice(&bincode::encode_to_vec(message_two, SERDE_CONFIG).unwrap());

        let mut mock = MockAsyncStream::new(vec![raw_data]);

        // 2. Act
        let result: Result<Vec<TestMessage>, IoError> = mock.deserialized_reads().await;

        // 3. Assert
        let deserialized = result.unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0], TestMessage { id: 1, data: "quick".to_string() });
        assert_eq!(deserialized[1], TestMessage { id: 2, data: "silver".to_string() });
    }
}
