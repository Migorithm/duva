use super::connected_peer_info::ConnectedPeerInfo;
use crate::domains::IoError;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::peers::connected_types::WriteConnected;
use crate::domains::query_parsers::QueryIO;
use crate::services::interface::TWrite;

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

    pub(crate) async fn write_io(&mut self, io: impl Into<QueryIO> + Send) -> Result<(), IoError> {
        self.w_conn.stream.write_io(io).await
    }

    pub(crate) async fn kill(self) -> OwnedReadHalf {
        self.listener_kill_trigger.kill().await
    }
}

#[derive(Clone, Debug)]
pub enum PeerKind {
    Replica { match_index: u64, replid: ReplicationId },
    NonDataPeer { replid: ReplicationId },
}

impl PeerKind {
    pub fn decide_peer_kind(my_repl_id: &ReplicationId, peer_info: &ConnectedPeerInfo) -> Self {
        if peer_info.replid == ReplicationId::Undecided {
            PeerKind::Replica { match_index: peer_info.hwm, replid: peer_info.replid.clone() }
        } else if my_repl_id == &peer_info.replid {
            PeerKind::Replica { match_index: peer_info.hwm, replid: peer_info.replid.clone() }
        } else {
            PeerKind::NonDataPeer { replid: peer_info.replid.clone() }
        }
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
