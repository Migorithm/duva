use std::marker::PhantomData;

use crate::services::cluster::peers::kind::PeerKind;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Debug)]
pub struct WriteConnected {
    pub stream: OwnedWriteHalf,
    pub kind: PeerKind,
}

#[derive(Debug)]
pub struct ReadConnected<T> {
    pub stream: OwnedReadHalf,
    pub kind: PhantomData<T>,
}

impl<T> ReadConnected<T> {
    pub fn new(stream: OwnedReadHalf) -> Self {
        Self { stream, kind: PhantomData }
    }
}

pub struct FromMaster;
pub struct FromSlave;
pub struct FromPeer;

impl From<(OwnedWriteHalf, PeerKind)> for WriteConnected {
    fn from((w, peer_kind): (OwnedWriteHalf, PeerKind)) -> WriteConnected {
        WriteConnected { stream: w, kind: peer_kind }
    }
}
