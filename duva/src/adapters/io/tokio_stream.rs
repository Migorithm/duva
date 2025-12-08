use crate::domains::peers::command::*;
use crate::domains::peers::connections::connection_types::{ReadConnection, WriteConnected};
use crate::domains::query_io::SERDE_CONFIG;
use crate::domains::{IoError, TAsyncReadWrite, TReadBytes, TSerdeDynamicWrite, TSerdeWrite};

use bytes::{Bytes, BytesMut};
use std::fmt::Debug;
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

// Arbitrary limit to prevent memory exhaustion.
const MAX_MSG_SIZE: usize = 4 * 1024 * 1024; // 4MB

impl<T: AsyncReadExt + std::marker::Unpin + Sync + Send + Debug + 'static> TReadBytes for T {
    // Reads a length-prefixed message from the stream.
    // The protocol is:
    // - 4 bytes (u32, big-endian) for the message length.
    // - N bytes for the message body, where N is the length read.
    async fn read_bytes(&mut self) -> Result<BytesMut, IoError> {
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

        // Reserve space in the buffer (Allocates, but doesn't write zeros yet)
        let mut buffer = BytesMut::with_capacity(len);

        // This ensures we never pull more than 'len' bytes from the stream,
        // even if the buffer has extra space.
        let mut handle = self.take(len as u64);

        // Unsafe-ish trick made safe by Tokio
        // Tokio's read_buf can read directly into uninitialized memory
        // preventing the "Double Write".
        while buffer.len() < len {
            let n = handle.read_buf(&mut buffer).await.map_err(|e| io_error_from_kind(e.kind()))?;
            if n == 0 {
                return Err(IoError::ConnectionAborted);
            }
        }

        Ok(buffer)
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

impl TAsyncReadWrite for TcpStream {
    async fn connect_and_split(
        connect_to: &str,
    ) -> Result<(ReadConnection<OwnedReadHalf>, WriteConnected), IoError> {
        let stream =
            TcpStream::connect(connect_to).await.map_err(|e| io_error_from_kind(e.kind()))?;
        let (r, w) = stream.into_split();
        Ok((ReadConnection::new(r), WriteConnected(Box::new(w))))
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
