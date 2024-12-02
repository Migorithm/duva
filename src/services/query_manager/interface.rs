use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};

use crate::services::interfaces::ThreadSafeCloneable;

pub trait TRead {
    fn read_bytes(
        &mut self,
        buf: &mut BytesMut,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

pub trait TWriteBuf {
    fn write_buf(
        &mut self,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

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

            // If no bytes are read, it suggests that the no more data will be received
            // for this message.
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

impl TWriteBuf for tokio::net::TcpStream {
    async fn write_buf(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        let stream = self as &mut tokio::net::TcpStream;
        stream.write_all(buf).await
    }
}

pub struct CancelNotifier {
    notifier: Option<tokio::sync::oneshot::Sender<()>>,
}
pub struct CancellationWatcher {
    receiver: tokio::sync::oneshot::Receiver<()>,
}

pub trait TCancellationTokenFactory<T, U>
where
    T: TCancelNotifier,
    U: TCancellationWatcher,
    Self: 'static,
{
    fn create() -> (T, U);
}

pub trait TCancelNotifier: Send + Sync + 'static {
    fn notify(self);
}
pub trait TCancellationWatcher: Send + Sync + 'static {
    fn watch(&mut self) -> bool;
}
impl TCancelNotifier for tokio::sync::oneshot::Sender<()> {
    fn notify(self) {
        let _ = self.send(());
    }
}
impl TCancellationTokenFactory<tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>>
    for (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )
{
    fn create() -> Self {
        tokio::sync::oneshot::channel()
    }
}

impl TCancellationWatcher for tokio::sync::oneshot::Receiver<()> {
    fn watch(&mut self) -> bool {
        self.try_recv().is_ok()
    }
}
