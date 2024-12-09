use bytes::BytesMut;

use super::error::IoError;

pub trait TStream: TRead + TWrite {}
impl<T> TStream for T where T: TRead + TWrite {}
pub trait TRead: Send + Sync + 'static {
    fn read_bytes(
        &mut self,
        buf: &mut BytesMut,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

pub trait TWrite: Send + Sync + 'static {
    fn write_all(
        &mut self,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}
pub trait TWriterFactory: TWrite + Send + Sync + 'static + Sized {
    fn create_writer(
        filepath: String,
    ) -> impl std::future::Future<Output = anyhow::Result<Self>> + Send;
}

pub trait TCancellationTokenFactory: Send + Sync + 'static {
    fn create(timeout: u64) -> Self;
    fn split(self) -> (impl TCancellationNotifier, impl TCancellationWatcher);
}

pub trait TCancellationNotifier: Send {
    fn notify(self);
}
pub trait TCancellationWatcher: Send {
    fn watch(&mut self) -> bool;
}

pub trait TSocketListener: Sync + Send + 'static {
    fn accept(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<(impl TWrite + TRead, std::net::SocketAddr), IoError>,
    > + Send;
}

pub trait TSocketListenerFactory: Sync + Send + 'static {
    fn create_listner(
        bind_addr: String,
    ) -> impl std::future::Future<Output = impl TSocketListener> + Send;
}
