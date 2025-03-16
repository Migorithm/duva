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
pub struct ReadConnected {
    pub stream: OwnedReadHalf,
}

impl ReadConnected {
    pub fn new(stream: OwnedReadHalf) -> Self {
        Self { stream }
    }
}

impl From<OwnedWriteHalf> for WriteConnected {
    fn from(w: OwnedWriteHalf) -> WriteConnected {
        WriteConnected { stream: w }
    }
}
