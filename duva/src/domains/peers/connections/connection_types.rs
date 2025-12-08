use anyhow::bail;
use bytes::Buf;
use bytes::BytesMut;
use tokio::io::AsyncReadExt;

use crate::domains::IoError;
use crate::domains::TSerdeDynamicRead;
use crate::domains::TSerdeDynamicWrite;
use crate::make_smart_pointer;

#[derive(Debug)]
pub(crate) struct WriteConnected(pub(crate) Box<dyn TSerdeDynamicWrite>);
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
pub(crate) struct ReadConnected(pub(crate) Box<dyn TSerdeDynamicRead>);

make_smart_pointer!(ReadConnected, Box<dyn TSerdeDynamicRead>);

impl<T: TSerdeDynamicRead> From<T> for ReadConnected {
    fn from(value: T) -> Self {
        Self(Box::new(value))
    }
}

impl PartialEq for ReadConnected {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ReadConnected {}

#[derive(Debug)]
pub(crate) struct ReadConnection<T>
where
    T: AsyncReadExt + std::marker::Unpin + Sync + Send + std::fmt::Debug + 'static,
{
    stream: T,
    buffer: BytesMut,
}

const MAX_MSG_SIZE: usize = 4 * 1024 * 1024; // 4MB

impl<T> ReadConnection<T>
where
    T: AsyncReadExt + std::marker::Unpin + Sync + Send + std::fmt::Debug + 'static,
{
    async fn read_bytes(&mut self) -> anyhow::Result<BytesMut> {
        loop {
            // 1. Attempt to parse what we already have
            if let Some(msg) = self.parse_frame()? {
                return Ok(msg);
            }

            // 2. If we don't have a full message, read more data
            // We assume 0 means connection closed
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // If we hit EOF but still have partial data, that's an error
                if self.buffer.is_empty() {
                    bail!("ConnectionAborted")
                } else {
                    bail!("UnexpectedEof")
                }
            }
        }
    }

    fn parse_frame(&mut self) -> anyhow::Result<Option<BytesMut>> {
        // Do we have enough for the length prefix?
        if self.buffer.len() < 4 {
            return Ok(None);
        }

        // Peek the length (without consuming bytes yet)
        let mut len_bytes = &self.buffer[..4];
        let len = len_bytes.get_u32() as usize;

        if len > MAX_MSG_SIZE {
            bail!("Incoming message too large: {len} bytes, max is {MAX_MSG_SIZE}");
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
}
