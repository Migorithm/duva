use std::ops::Deref;
use std::ops::DerefMut;

use crate::services::cluster::peers::connected_types::WriteConnected;

use tokio::net::tcp::OwnedWriteHalf;

use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::connected_types::ReadConnected;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: Instant,
}

impl Peer {
    pub fn new(
        write_connected: WriteConnected,
        listener_kill_trigger: ListeningActorKillTrigger,
    ) -> Self {
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
pub(crate) struct ListeningActorKillTrigger(KillTrigger, JoinHandle<ReadConnected>);
impl ListeningActorKillTrigger {
    pub(crate) fn new(kill_trigger: KillTrigger, listning_task: JoinHandle<ReadConnected>) -> Self {
        Self(kill_trigger, listning_task)
    }
    pub(crate) async fn kill(self) -> ReadConnected {
        let _ = self.0.send(());
        self.1.await.unwrap()
    }
}
