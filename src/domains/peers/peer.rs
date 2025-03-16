use super::connected_peer_info::ConnectedPeerInfo;
use super::identifier::PeerIdentifier;
use crate::domains::IoError;
use crate::domains::peers::connected_types::WriteConnected;
use crate::domains::query_parsers::QueryIO;
use crate::services::interface::TWrite;
use bytes::Bytes;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) addr: String,
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: Instant,
    pub kind: PeerKind,
}

impl Peer {
    pub fn new(
        addr: String,
        w: OwnedWriteHalf,
        kind: PeerKind,
        listener_kill_trigger: ListeningActorKillTrigger,
    ) -> Self {
        Self {
            addr,
            w_conn: WriteConnected::new(w),
            listener_kill_trigger,
            last_seen: Instant::now(),
            kind,
        }
    }

    pub(crate) async fn write(&mut self, buf: impl Into<Bytes> + Send) -> Result<(), IoError> {
        self.w_conn.stream.write(buf).await
    }

    pub(crate) async fn write_io(&mut self, io: impl Into<QueryIO> + Send) -> Result<(), IoError> {
        self.w_conn.stream.write_io(io).await
    }

    pub(crate) async fn kill(self) -> OwnedReadHalf {
        self.listener_kill_trigger.kill().await
    }
}

#[derive(Clone, Debug)]
pub enum PeerKind {
    Follower { watermark: u64, leader_repl_id: PeerIdentifier },
    Leader,
    PFollower { leader_repl_id: PeerIdentifier },
    PLeader,
}

impl PeerKind {
    pub fn decide_peer_kind(my_repl_id: &str, peer_info: ConnectedPeerInfo) -> Self {
        if my_repl_id == *peer_info.id {
            return Self::Leader;
        }
        if my_repl_id == *peer_info.leader_repl_id {
            return Self::Follower {
                watermark: peer_info.hwm,
                leader_repl_id: peer_info.leader_repl_id,
            };
        }
        if peer_info.id == peer_info.leader_repl_id {
            return Self::PLeader;
        }
        Self::PFollower { leader_repl_id: peer_info.leader_repl_id }
    }
}

#[derive(Debug)]
pub(crate) struct ListeningActorKillTrigger(
    tokio::sync::oneshot::Sender<()>,
    JoinHandle<OwnedReadHalf>,
);
impl ListeningActorKillTrigger {
    pub(crate) fn new(
        kill_trigger: tokio::sync::oneshot::Sender<()>,
        listning_task: JoinHandle<OwnedReadHalf>,
    ) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> OwnedReadHalf {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}
