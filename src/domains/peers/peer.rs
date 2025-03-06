use super::connected_types::Follower;
use super::connected_types::Leader;
use super::connected_types::NonDataPeer;
use super::connected_types::ReadConnected;
use super::kind::PeerKind;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_listeners::ClusterListener;
use crate::domains::cluster_listeners::TListen;
use crate::domains::peers::connected_types::WriteConnected;
use std::ops::Deref;
use std::ops::DerefMut;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
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
        let w_conn = WriteConnected::new(w);

        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let rc = ReadConnected::<T>::new(r);
        let listening_actor = ClusterListener::new(rc, cluster_handler);
        let listener_kill_trigger = ListeningActorKillTrigger::new(
            kill_trigger,
            tokio::spawn(listening_actor.listen(kill_switch)),
        );
        Self { w_conn, listener_kill_trigger, last_seen: Instant::now(), kind }
    }

    pub(crate) fn create(
        stream: TcpStream,
        kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Peer {
        match kind {
            PeerKind::Follower { .. } => {
                Peer::new::<Follower>(stream, kind, cluster_handler)
            }
            PeerKind::Leader => {
                Peer::new::<Leader>(stream, kind, cluster_handler)
            }
            _ => Peer::new::<NonDataPeer>(stream, kind, cluster_handler),
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
