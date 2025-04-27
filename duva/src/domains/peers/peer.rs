use crate::domains::IoError;
use crate::domains::cluster_actors::replication::ReplicationId;
use crate::domains::peers::connected_types::WriteConnected;
use crate::domains::query_parsers::QueryIO;
use crate::services::interface::TWrite;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::cluster_peer::NodeKind;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) addr: String,
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: Instant,
    pub(crate) peer_state: PeerState,
}

impl Peer {
    pub(crate) fn new(
        addr: String,
        w: OwnedWriteHalf,
        state: PeerState,
        listener_kill_trigger: ListeningActorKillTrigger,
    ) -> Self {
        Self {
            addr,
            w_conn: WriteConnected::new(w),
            listener_kill_trigger,
            last_seen: Instant::now(),
            peer_state: state,
        }
    }

    pub(crate) fn kind(&self) -> &NodeKind {
        &self.peer_state.node_kind
    }
    pub(crate) fn match_index(&self) -> u64 {
        self.peer_state.match_index
    }

    pub(crate) async fn send_to_peer(
        &mut self,
        io: impl Into<QueryIO> + Send,
    ) -> Result<(), IoError> {
        self.w_conn.stream.write_io(io).await
    }

    pub(crate) async fn kill(self) -> OwnedReadHalf {
        self.listener_kill_trigger.kill().await
    }
}

#[derive(Clone, Debug)]
pub struct PeerState {
    pub(crate) match_index: u64,
    pub(crate) replid: ReplicationId,
    pub(crate) node_kind: NodeKind,
}

impl PeerState {
    pub(crate) fn new(match_index: u64, replid: ReplicationId, node_kind: NodeKind) -> Self {
        Self { match_index, replid, node_kind }
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
