use super::connected_peer_info::ConnectedPeerInfo;
use super::connected_types::Follower;
use super::connected_types::Leader;
use super::connected_types::NonDataPeer;
use super::connected_types::ReadConnected;
use super::identifier::PeerIdentifier;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_listeners::ClusterListener;
use crate::domains::cluster_listeners::TListen;
use crate::domains::peers::connected_types::WriteConnected;
use std::fmt::Display;
use std::ops::Deref;
use std::ops::DerefMut;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: Instant,
    pub kind: PeerKind,
}

impl Peer {
    pub fn new<T>(
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
            w_conn: WriteConnected::new(w),
            listener_kill_trigger,
            last_seen: Instant::now(),
            kind,
        }
    }

    pub(crate) fn create(
        stream: TcpStream,
        kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Peer {
        match kind {
            PeerKind::Follower { .. } => Peer::new::<Follower>(stream, kind, cluster_handler),
            PeerKind::Leader => Peer::new::<Leader>(stream, kind, cluster_handler),
            _ => Peer::new::<NonDataPeer>(stream, kind, cluster_handler),
        }
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

impl Display for PeerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerKind::Follower { watermark: hwm, leader_repl_id } => {
                write!(f, "follower {}", leader_repl_id)
            },
            PeerKind::Leader => write!(f, "leader - 0"),
            PeerKind::PFollower { leader_repl_id } => write!(f, "follower {}", leader_repl_id),
            PeerKind::PLeader => write!(f, "leader - 0"),
        }
    }
}

impl Deref for Peer {
    type Target = OwnedWriteHalf;

    fn deref(&self) -> &Self::Target {
        &self.w_conn.stream
    }
}

impl DerefMut for Peer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.w_conn.stream
    }
}

pub(super) type KillTrigger = tokio::sync::oneshot::Sender<()>;

#[derive(Debug)]
pub(crate) struct ListeningActorKillTrigger(KillTrigger, JoinHandle<OwnedReadHalf>);
impl ListeningActorKillTrigger {
    pub(crate) fn new(kill_trigger: KillTrigger, listning_task: JoinHandle<OwnedReadHalf>) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> OwnedReadHalf {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}
