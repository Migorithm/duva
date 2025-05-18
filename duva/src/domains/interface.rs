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

pub trait TSerdeReadWrite {
    fn serialized_write(
        &mut self,
        buf: impl bincode::Encode + Send,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;

    fn deserialized_read<U: bincode::Decode<()> + Send>(
        &mut self,
    ) -> impl std::future::Future<Output = Result<U, IoError>> + Send;
}

pub trait TGetPeerIp {
    fn get_peer_ip(&self) -> Result<String, IoError>;
}
