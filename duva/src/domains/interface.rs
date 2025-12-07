use std::fmt::Debug;

use crate::domains::{
    IoError,
    peers::{
        command::PeerMessage,
        connections::connection_types::{ReadConnected, WriteConnected},
    },
};
use bytes::{Bytes, BytesMut};

pub trait TReadBytes: Send + Sync + Debug + 'static {
    fn read_bytes(&mut self)
    -> impl std::future::Future<Output = Result<BytesMut, IoError>> + Send;
}

#[async_trait::async_trait]
pub(crate) trait TSerdeDynamicRead: Send + Sync + Debug + 'static {
    async fn receive_peer_msgs(&mut self) -> Result<PeerMessage, IoError>;
    async fn receive_connection_msgs(&mut self) -> Result<String, IoError>;
}

#[async_trait::async_trait]
pub(crate) trait TSerdeDynamicWrite: Send + Sync + Debug + 'static {
    async fn send(&mut self, msg: PeerMessage) -> Result<(), IoError>;
    async fn send_connection_msg(&mut self, arg: &str) -> Result<(), IoError>;
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
}

pub(crate) trait TAsyncReadWrite {
    fn connect(
        connect_to: &str,
    ) -> impl std::future::Future<Output = Result<(ReadConnected, WriteConnected), IoError>> + Send;
}
