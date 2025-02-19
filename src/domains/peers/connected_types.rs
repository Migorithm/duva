use std::marker::PhantomData;

use crate::domains::peers::kind::PeerKind;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Debug)]
pub struct WriteConnected {
    pub stream: OwnedWriteHalf,
    pub kind: PeerKind,
}
impl WriteConnected {
    pub fn new(stream: OwnedWriteHalf, kind: PeerKind) -> Self {
        Self { stream, kind }
    }
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

pub struct Leader;
pub struct Follower;
pub struct NonDataPeer;

impl From<(OwnedWriteHalf, PeerKind)> for WriteConnected {
    fn from((w, peer_kind): (OwnedWriteHalf, PeerKind)) -> WriteConnected {
        WriteConnected { stream: w, kind: peer_kind }
    }
}
