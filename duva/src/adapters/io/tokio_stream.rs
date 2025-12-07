use crate::domains::peers::command::*;
use crate::domains::peers::connections::connection_types::{ReadConnected, WriteConnected};
use crate::domains::query_io::SERDE_CONFIG;
use crate::domains::{
    IoError, TAsyncReadWrite, TReadBytes, TSerdeDynamicRead, TSerdeDynamicWrite, TSerdeRead,
    TSerdeWrite,
};

use bytes::BytesMut;
use std::fmt::Debug;
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const INITIAL_CAPACITY: usize = 1024;
// Arbitrary limit to prevent memory exhaustion.
const MAX_MSG_SIZE: usize = 4 * 1024 * 1024; // 4MB

impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TReadBytes for T {
    // Reads a length-prefixed message from the stream.
    // The protocol is:
    // - 4 bytes (u32, big-endian) for the message length.
    // - N bytes for the message body, where N is the length read.
    async fn read_bytes(&mut self, buffer: &mut BytesMut) -> Result<(), IoError> {
        let len = self.read_u32().await.map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                IoError::ConnectionAborted
            } else {
                io_error_from_kind(e.kind())
            }
        })? as usize;

        if len > MAX_MSG_SIZE {
            return Err(IoError::Custom(format!(
                "Incoming message too large: {len} bytes, max is {MAX_MSG_SIZE}"
            )));
        }

        buffer.reserve(len);
        let mut body_buffer = vec![0u8; len];
        if let Err(e) = self.read_exact(&mut body_buffer).await {
            return if e.kind() == ErrorKind::UnexpectedEof {
                Err(IoError::ConnectionAborted)
            } else {
                Err(io_error_from_kind(e.kind()))
            };
        }

        buffer.extend_from_slice(&body_buffer);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeDynamicRead for T {
    async fn receive_peer_msgs(&mut self) -> Result<PeerMessage, IoError> {
        let mut buffer = BytesMut::with_capacity(INITIAL_CAPACITY);
        self.read_bytes(&mut buffer).await?;

        let (peer_message, _) = bincode::decode_from_slice(&buffer, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;

        Ok(peer_message)
    }
    async fn receive_connection_msgs(&mut self) -> Result<String, IoError> {
        self.deserialized_read().await
    }
}

impl<T: AsyncWriteExt + std::marker::Unpin + Sync + Send + Debug + 'static> TSerdeWrite for T {
    async fn serialized_write(&mut self, buf: impl bincode::Encode + Send) -> Result<(), IoError> {
        let encoded = bincode::encode_to_vec(buf, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;

        let len = encoded.len() as u32;
        self.write_u32(len).await.map_err(|e| io_error_from_kind(e.kind()))?;

        self.write_all(&encoded).await.map_err(|e| io_error_from_kind(e.kind()))?;
        self.flush().await.map_err(|e| io_error_from_kind(e.kind()))
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
        self.write_u32(len).await.map_err(|e| io_error_from_kind(e.kind()))?;

        self.write_all(&encoded).await.map_err(|e| io_error_from_kind(e.kind()))?;
        self.flush().await.map_err(|e| io_error_from_kind(e.kind()))
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
    use super::*;
    #[derive(Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct TestMessage {
        id: u32,
        data: String,
    }

    /// A mock that implements AsyncRead for testing by simulating a byte stream.
    #[derive(Debug)]
    struct MockAsyncStream {
        data: Vec<u8>,
        pos: usize,
    }

    impl MockAsyncStream {
        /// Creates a new mock stream from a vector of byte chunks, which are flattened.
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            MockAsyncStream { data: chunks.into_iter().flatten().collect(), pos: 0 }
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

            if self_mut.pos >= self_mut.data.len() {
                return std::task::Poll::Ready(Ok(())); // EOF
            }

            let remaining_data = &self_mut.data[self_mut.pos..];
            let bytes_to_copy = std::cmp::min(buf.remaining(), remaining_data.len());

            buf.put_slice(&remaining_data[..bytes_to_copy]);
            self_mut.pos += bytes_to_copy;

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
    async fn test_deserialize_reads() {
        // 1. Arrange: Single message in one chunk
        let msg = TestMessage { id: 1, data: "quick".to_string() };

        let encoded = bincode::encode_to_vec(&msg, SERDE_CONFIG).unwrap();
        let len = encoded.len() as u32;
        let mut framed_msg = len.to_be_bytes().to_vec();
        framed_msg.extend_from_slice(&encoded);

        let mut mock = MockAsyncStream::new(vec![framed_msg]);

        // 2. Act
        let result: Result<TestMessage, IoError> = mock.deserialized_read().await;

        // 3. Assert
        let deserialized = result.unwrap();

        assert_eq!(deserialized, msg);
    }

    #[tokio::test]
    async fn test_deserialize_reads_vec() {
        // 1. Arrange: two messages sent sequentially
        let message_one = TestMessage { id: 1, data: "quick".to_string() };
        let message_two = TestMessage { id: 2, data: "silver".to_string() };

        let encoded1 = bincode::encode_to_vec(&message_one, SERDE_CONFIG).unwrap();
        let len1 = encoded1.len() as u32;
        let mut framed_msg1 = len1.to_be_bytes().to_vec();
        framed_msg1.extend_from_slice(&encoded1);

        let encoded2 = bincode::encode_to_vec(&message_two, SERDE_CONFIG).unwrap();
        let len2 = encoded2.len() as u32;
        let mut framed_msg2 = len2.to_be_bytes().to_vec();
        framed_msg2.extend_from_slice(&encoded2);

        let mut combined_data = framed_msg1;
        combined_data.extend_from_slice(&framed_msg2);

        let mut mock = MockAsyncStream::new(vec![combined_data]);

        // 2. Act: read first message
        let result1: Result<TestMessage, IoError> = mock.deserialized_read().await;

        // 3. Assert: first message is correct
        let deserialized1 = result1.unwrap();

        assert_eq!(deserialized1, message_one);

        // 4. Act: read second message
        let result2: Result<TestMessage, IoError> = mock.deserialized_read().await;

        // 5. Assert: second message is correct
        let deserialized2 = result2.unwrap();

        assert_eq!(deserialized2, message_two);
    }
}
