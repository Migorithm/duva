use std::io::ErrorKind;
use std::ops::Deref;
use std::ops::DerefMut;

use crate::domains::IoError;

use crate::domains::TSerdeDynamicWrite;

use crate::domains::peers::command::PeerMessage;
use crate::domains::query_io::SERDE_CONFIG;
use crate::make_smart_pointer;

use bytes::Buf;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;

const MAX_MSG_SIZE: usize = 4 * 1024 * 1024; // 4MB

// TODO: from DP -> OwnedType
#[derive(Debug)]
pub struct WriteConnected(pub(crate) Box<dyn TSerdeDynamicWrite>);
make_smart_pointer!(WriteConnected, Box<dyn TSerdeDynamicWrite>);

impl<T: TSerdeDynamicWrite> From<T> for WriteConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

impl PartialEq for WriteConnected {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for WriteConnected {}

#[derive(Debug)]
pub struct ReadConnection<T> {
    pub(crate) stream: T,
    // Persistent Buffer - instead of creating a new buffer for every message,
    // we keep one buffer
    buffer: BytesMut,
}

impl<T> PartialEq for ReadConnection<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for ReadConnection<T> {}
impl<T> Deref for ReadConnection<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl<T> DerefMut for ReadConnection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl<T> ReadConnection<T>
where
    T: AsyncReadExt + std::marker::Unpin + Sync + Send + std::fmt::Debug + 'static,
{
    pub fn new(stream: T) -> Self {
        Self { stream, buffer: BytesMut::with_capacity(1024) }
    }
    pub async fn read_bytes(&mut self) -> Result<BytesMut, std::io::Error> {
        loop {
            // 1. Attempt to parse what we already have (consumer)
            if let Some(msg) = self.parse_frame()? {
                return Ok(msg);
            }

            // 2. If we don't have a full message, read more data (producer)
            // We assume 0 means connection closed
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // If we hit EOF but still have partial data, that's an error
                if self.buffer.is_empty() {
                    return Err(std::io::Error::new(ErrorKind::ConnectionAborted, ""));
                } else {
                    return Err(std::io::Error::new(ErrorKind::UnexpectedEof, ""));
                }
            }
        }
    }

    pub fn parse_frame(&mut self) -> Result<Option<BytesMut>, std::io::Error> {
        // Do we have enough for the length prefix?
        if self.buffer.len() < 4 {
            return Ok(None);
        }

        // Peek the length (without consuming bytes yet)
        let mut len_bytes = &self.buffer[..4];
        let len = len_bytes.get_u32() as usize;

        if len > MAX_MSG_SIZE {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("Incoming message too large: {len} bytes, max is {MAX_MSG_SIZE}"),
            ));
        }

        // Do we have the full message? (4 bytes header + len bytes body)
        if self.buffer.len() < 4 + len {
            return Ok(None); // Not enough data yet, go back to reading
        }

        // SUCCESS!
        self.buffer.advance(4); // Consume header
        let msg = self.buffer.split_to(len); // Consume body (Zero Copy split!)

        // Whatever is left in self.buffer stays there for the next call.
        Ok(Some(msg))
    }

    pub async fn read_bincode<U>(&mut self) -> Result<U, IoError>
    where
        U: bincode::Decode<()>,
    {
        let body = self.read_bytes().await.map_err(|e| e.kind())?;
        let (request, _) = bincode::decode_from_slice(&body, SERDE_CONFIG)
            .map_err(|e| IoError::Custom(e.to_string()))?;
        Ok(request)
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
                eprintln!("unknown error: {value:?}");
                IoError::Custom(format!("unknown error: {value:?}"))
            },
        }
    }
}
