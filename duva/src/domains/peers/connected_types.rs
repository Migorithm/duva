use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Debug)]
pub(crate) struct WriteConnected {
    pub(crate) stream: OwnedWriteHalf,
}
impl WriteConnected {
    pub(crate) fn new(stream: OwnedWriteHalf) -> Self {
        Self { stream }
    }
}

#[derive(Debug)]
pub(crate) struct ReadConnected {
    pub(crate) stream: OwnedReadHalf,
}

impl ReadConnected {
    pub(crate) fn new(stream: OwnedReadHalf) -> Self {
        Self { stream }
    }
}

impl From<OwnedWriteHalf> for WriteConnected {
    fn from(w: OwnedWriteHalf) -> WriteConnected {
        WriteConnected { stream: w }
    }
}
