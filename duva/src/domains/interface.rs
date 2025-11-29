use std::fmt::Debug;

use crate::domains::{
    IoError,
    peers::{
        command::PeerMessage,
        connections::connection_types::{ReadConnected, WriteConnected},
    },
};
use bincode::BorrowDecode;
use bytes::BytesMut;

#[async_trait::async_trait]
pub trait TReadBytes: Send + Sync + Debug + 'static {
    async fn read_bytes(&mut self, buf: &mut BytesMut) -> Result<(), IoError>;
}

#[async_trait::async_trait]
pub(crate) trait TSerdeDynamicRead: Send + Sync + Debug + 'static {
    async fn receive_peer_msgs(&mut self) -> Result<Vec<PeerMessage>, IoError>;
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

#[async_trait::async_trait]
pub trait TSerdeRead {
    async fn deserialized_read<'a, U>(&mut self, buffer: &'a mut BytesMut) -> Result<U, IoError>
    where
        U: BorrowDecode<'a, ()> + Send; // 'U' lives as long as 'buffer'

    async fn deserialized_reads<'a, U>(
        &mut self,
        buffer: &'a mut BytesMut,
    ) -> Result<Vec<U>, IoError>
    where
        U: BorrowDecode<'a, ()> + Send;
}

pub(crate) trait TAsyncReadWrite {
    fn connect(
        connect_to: &str,
    ) -> impl std::future::Future<Output = Result<(ReadConnected, WriteConnected), IoError>> + Send;
}
