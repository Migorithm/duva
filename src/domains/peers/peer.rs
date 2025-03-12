use super::connected_peer_info::ConnectedPeerInfo;
use super::connected_types::Follower;
use super::connected_types::Leader;
use super::connected_types::NonDataPeer;
use super::connected_types::ReadConnected;
use super::identifier::PeerIdentifier;
use crate::domains::IoError;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_listeners::ClusterListener;
use crate::domains::cluster_listeners::TListen;
use crate::domains::peers::connected_types::WriteConnected;
use crate::domains::query_parsers::QueryIO;
use crate::services::interface::TWrite;
use bytes::Bytes;
use std::fmt::Display;

use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

use tokio::sync::mpsc::Sender;
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
    pub fn new<T>(
        addr: String,
        stream: TcpStream,
        kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Self
    where
        ClusterListener<T>: TListen + Send + Sync + 'static,
    {
        let (r, w) = stream.into_split();
        let listening_actor = ClusterListener::new(ReadConnected::<T>::new(r), cluster_handler);

        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let listener_kill_trigger = ListeningActorKillTrigger::new(
            kill_trigger,
            tokio::spawn(listening_actor.listen(kill_switch)),
        );
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

    pub(crate) fn create(
        addr: String,
        stream: TcpStream,
        kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Peer {
        match kind {
            PeerKind::Follower { .. } => Peer::new::<Follower>(addr, stream, kind, cluster_handler),
            PeerKind::Leader => Peer::new::<Leader>(addr, stream, kind, cluster_handler),
            _ => Peer::new::<NonDataPeer>(addr, stream, kind, cluster_handler),
        }
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
