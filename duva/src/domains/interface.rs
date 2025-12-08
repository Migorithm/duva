use std::fmt::Debug;

use crate::domains::{
    IoError,
    peers::{
        command::PeerMessage,
        connections::connection_types::{ReadConnection, WriteConnected},
    },
};
use bytes::{Bytes, BytesMut};
use tokio::net::tcp::OwnedReadHalf;

pub trait TReadBytes: Send + Sync + Debug + 'static {
    fn read_bytes(&mut self)
    -> impl std::future::Future<Output = Result<BytesMut, IoError>> + Send;
}

// deprecated
#[async_trait::async_trait]
pub trait TSerdeDynamicRead: Send + Sync + Debug + 'static {
    async fn receive_peer_msgs(&mut self) -> Result<PeerMessage, IoError>;
    async fn receive_connection_msgs(&mut self) -> Result<String, IoError>;
}

#[async_trait::async_trait]
pub trait TSerdeDynamicWrite: Send + Sync + Debug + 'static {
    async fn send(&mut self, msg: PeerMessage) -> Result<(), IoError>;
    async fn send_connection_msg(&mut self, arg: &str) -> Result<(), IoError>;
}

pub trait TSerdeWrite {
    fn serialized_write(
        &mut self,
        buf: impl bincode::Encode + Send,
    ) -> impl std::future::Future<Output = Result<(), IoError>> + Send;
}

pub trait TAsyncReadWrite {
    fn connect_and_split(
        connect_to: &str,
    ) -> impl std::future::Future<
        Output = Result<(ReadConnection<OwnedReadHalf>, WriteConnected), IoError>,
    > + Send;
}
