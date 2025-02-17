use std::ops::Deref;
use std::ops::DerefMut;

use super::connected_types::ReadConnected;
use super::identifier::PeerIdentifier;

use crate::services::cluster::actors::commands::ClusterCommand;
use crate::services::cluster::peer_listeners::PeerListener;
use crate::services::cluster::peer_listeners::TListen;
use crate::services::cluster::peers::connected_types::WriteConnected;
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;
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
}

impl Peer {
    pub fn new<T>(
        write_connected: WriteConnected,
        read_connected: OwnedReadHalf,
        cluster_handler: Sender<ClusterCommand>,
        addr: PeerIdentifier,
        snapshot_applier: SnapshotApplier,
    ) -> Self
    where
        PeerListener<T>: TListen + Send + Sync + 'static,
    {
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let rc = ReadConnected::<T>::new(read_connected);
        let listening_actor = PeerListener::new(rc, cluster_handler, addr, snapshot_applier);
        let listener_kill_trigger = ListeningActorKillTrigger::new(
            kill_trigger,
            tokio::spawn(listening_actor.listen(kill_switch)),
        );
        Self { w_conn: write_connected, listener_kill_trigger, last_seen: Instant::now() }
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
