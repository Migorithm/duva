use std::fmt::Debug;

use crate::domains::{IoError, query_parsers::QueryIO};
use bytes::{Bytes, BytesMut};

#[async_trait::async_trait]
pub trait TRead: Send + Sync + Debug + 'static {
    async fn read_bytes(&mut self, buf: &mut BytesMut) -> Result<(), IoError>;

    async fn read_values(&mut self) -> Result<Vec<QueryIO>, IoError>;
}

#[async_trait::async_trait]
pub(crate) trait TWrite: Send + Sync + Debug + 'static {
    async fn write(&mut self, buf: Bytes) -> Result<(), IoError>;

    async fn write_io(&mut self, io: QueryIO) -> Result<(), IoError>;
}

#[async_trait::async_trait]
pub trait TSerdeReadWrite {
    async fn serialized_write(&mut self, buf: impl bincode::Encode + Send) -> Result<(), IoError>;

    async fn deserialized_read<U: bincode::Decode<()> + Send>(&mut self) -> Result<U, IoError>;
}

pub trait TGetPeerIp {
    fn get_peer_ip(&self) -> Result<String, IoError>;
}
