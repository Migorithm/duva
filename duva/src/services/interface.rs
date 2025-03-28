use crate::domains::{IoError, query_parsers::QueryIO};
use bytes::{Bytes, BytesMut};

pub trait TRead {
    fn read_bytes(
        &mut self,
        buf: &mut BytesMut,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;

    fn read_values(&mut self) -> impl std::future::Future<Output = Result<Vec<QueryIO>, IoError>>;
}

pub(crate) trait TWrite {
    fn write(
        &mut self,
        buf: impl Into<Bytes> + Send,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;

    fn write_io(
        &mut self,
        io: impl Into<QueryIO> + Send,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}

pub trait TSerWrite {
    fn ser_write(
        &mut self,
        buf: impl bincode::Encode + Send,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}

pub trait TAuthRead<T: bincode::Decode<()> + Send> {
    fn auth_read(&mut self) -> impl std::future::Future<Output = Result<T, IoError>> + Send;
}

pub trait TGetPeerIp {
    fn get_peer_ip(&self) -> Result<String, IoError>;
}
