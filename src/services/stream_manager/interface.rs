use super::{error::IoError, query_io::QueryIO};
use bytes::BytesMut;

pub trait TStream: TGetPeerIp + Send + Sync + 'static {
    // TODO deprecated
    fn read_value(&mut self) -> impl std::future::Future<Output = anyhow::Result<QueryIO>> + Send;
    fn read_values(&mut self) -> impl std::future::Future<Output = anyhow::Result<Vec<QueryIO>>>;
    fn write(
        &mut self,
        value: QueryIO,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}

pub(crate) trait TExtractQuery<R, A> {
    fn extract_query(&mut self)
        -> impl std::future::Future<Output = anyhow::Result<(R, A)>> + Send;
}

pub(crate) trait TRead {
    fn read_bytes(
        &mut self,
        buf: &mut BytesMut,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}

pub(crate) trait TWrite {
    fn write(
        &mut self,
        buf: &[u8],
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}
pub(crate) trait TWriterFactory: TWrite + Send + Sync + 'static + Sized {
    fn create_writer(
        filepath: String,
    ) -> impl std::future::Future<Output = anyhow::Result<Self>> + Send;
}

pub trait TCancellationTokenFactory: Send + Sync + Copy + 'static {
    fn create(&self, timeout: u64) -> (impl TCancellationNotifier, impl TCancellationWatcher);
}

pub trait TCancellationNotifier: Send {
    fn notify(self);
}
pub trait TCancellationWatcher: Send {
    fn watch(&mut self) -> bool;
}

pub trait TGetPeerIp {
    fn get_peer_ip(&self) -> Result<String, IoError>;
}
