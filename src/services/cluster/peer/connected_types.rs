use crate::services::cluster::peer::kind::PeerKind;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Debug)]
pub struct WriteConnected {
    pub stream: OwnedWriteHalf,
    pub kind: PeerKind,
}

#[derive(Debug)]
pub struct ReadConnected {
    pub stream: OwnedReadHalf,
    pub kind: PeerKind,
}

impl From<(OwnedReadHalf, PeerKind)> for ReadConnected {
    fn from((r, peer_kind): (OwnedReadHalf, PeerKind)) -> ReadConnected {
        ReadConnected { stream: r, kind: peer_kind }
    }
}

impl From<(OwnedWriteHalf, PeerKind)> for WriteConnected {
    fn from((w, peer_kind): (OwnedWriteHalf, PeerKind)) -> WriteConnected {
        WriteConnected { stream: w, kind: peer_kind }
    }
}