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
    pub kind: PeerState,
}

impl Peer {
    pub fn new(
        addr: String,
        w: OwnedWriteHalf,
        kind: PeerState,
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
pub enum PeerState {
    Replica { match_index: u64, replid: ReplicationId },
    NonDataPeer { match_index: u64, replid: ReplicationId },
}

impl PeerState {
    pub(crate) fn decide_peer_kind(
        my_repl_id: &ReplicationId,
        peer_info: &ConnectedPeerInfo,
    ) -> Self {
        match (my_repl_id, &peer_info.replid) {
            // Peer is undecided - assign as replica with our replication ID
            (_, ReplicationId::Undecided) => {
                PeerState::Replica { match_index: peer_info.hwm, replid: my_repl_id.clone() }
            },
            // I am undecided - adopt peer's replication ID
            (ReplicationId::Undecided, _) => {
                PeerState::Replica { match_index: peer_info.hwm, replid: peer_info.replid.clone() }
            },
            // Matching replication IDs - regular replica
            (my_id, peer_id) if my_id == peer_id => {
                PeerState::Replica { match_index: peer_info.hwm, replid: peer_info.replid.clone() }
            },
            // Different replication IDs - non-data peer
            _ => PeerState::NonDataPeer {
                match_index: peer_info.hwm,
                replid: peer_info.replid.clone(),
            },
        }
    }

    pub(crate) fn decrease_match_index(&mut self) {
        match self {
            PeerState::Replica { match_index, .. } => *match_index -= 1,
            PeerState::NonDataPeer { match_index, .. } => *match_index -= 1,
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
