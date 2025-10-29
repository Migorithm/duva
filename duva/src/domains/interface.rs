use std::fmt::Debug;

use crate::domains::{
    IoError, QueryIO,
    peers::connections::connection_types::{ReadConnected, WriteConnected},
    replications::messages::PeerMessage,
};
use bytes::BytesMut;

#[async_trait::async_trait]
pub trait TRead: Send + Sync + Debug + 'static {
    async fn read_values(&mut self) -> Result<Vec<QueryIO>, IoError>;
}

#[async_trait::async_trait]
pub trait TReadBytes: Send + Sync + Debug + 'static {
    async fn read_bytes(&mut self, buf: &mut BytesMut) -> Result<(), IoError>;
}

#[async_trait::async_trait]
pub(crate) trait TSerdeDynamicRead: TRead + Send + Sync + Debug + 'static {
    async fn receive_peer_msgs(&mut self) -> Result<Vec<PeerMessage>, IoError>;
}

#[async_trait::async_trait]
pub(crate) trait TSerdeDynamicWrite: TWrite + Send + Sync + Debug + 'static {
    async fn send(&mut self, msg: PeerMessage) -> Result<(), IoError>;
}

#[async_trait::async_trait]
pub(crate) trait TWrite: Send + Sync + Debug + 'static {
    async fn write(&mut self, io: QueryIO) -> Result<(), IoError>;
}

pub trait TSerdeWrite {
    fn serialized_write(
        &mut self,
        buf: impl bincode::Encode + Send,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}

pub trait TSerdeRead {
    fn deserialized_read<U: bincode::Decode<()>>(
        &mut self,
    ) -> impl std::future::Future<Output = Result<U, IoError>> + Send;

    fn deserialized_reads<U: bincode::Decode<()>>(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Vec<U>, IoError>> + Send;
}

pub(crate) trait TAsyncReadWrite {
    fn connect(
        connect_to: &str,
    ) -> impl std::future::Future<Output = Result<(ReadConnected, WriteConnected), IoError>> + Send;
}
