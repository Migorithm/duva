use std::marker::PhantomData;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Debug)]
pub struct WriteConnected {
    pub stream: OwnedWriteHalf,
}
impl WriteConnected {
    pub fn new(stream: OwnedWriteHalf) -> Self {
        Self { stream }
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

impl From<OwnedWriteHalf> for WriteConnected {
    fn from(w: OwnedWriteHalf) -> WriteConnected {
        WriteConnected { stream: w }
    }
}
