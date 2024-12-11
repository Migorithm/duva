use bytes::BytesMut;

use super::{error::IoError, PeerAddr};

pub trait TStream: TRead + TWrite + TGetPeerIp {}
impl<T> TStream for T where T: TRead + TWrite + TGetPeerIp {}

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
    fn create(timeout: u64) -> (impl TCancellationNotifier, impl TCancellationWatcher);
}

pub trait TCancellationNotifier: Send {
    fn notify(self);
}
pub trait TCancellationWatcher: Send {
    fn watch(&mut self) -> bool;
}

pub trait TListenStream: Sync + Send + 'static {
    fn accept(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<(impl TStream, std::net::SocketAddr), IoError>,
    > + Send;
}

pub trait TGetPeerIp: Send + Sync + 'static {
    fn get_peer_ip(&self) -> Result<String, IoError>;
}

pub trait TCreateStreamListener: Sync + Send + 'static {
    fn create_listner(
        bind_addr: String,
    ) -> impl std::future::Future<Output = impl TListenStream> + Send;
}

pub trait TConnectStream: Sync + Send + 'static {
    fn connect(
        addr: PeerAddr,
    ) -> impl std::future::Future<Output = Result<impl TStream, IoError>> + Send;
}
