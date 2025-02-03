use super::types::PeerIdentifier;
use crate::services::cluster::command::ClusterCommand;
use crate::services::cluster::actors::listening_actor::ListeningActorKillTrigger;
use crate::services::cluster::actors::listening_actor::PeerListeningActor;
use crate::services::cluster::actors::types::PeerKind;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listner_kill_trigger: ListeningActorKillTrigger,
    pub(crate) last_seen: std::time::Instant,
}

impl Peer {
    pub fn new(
        stream: TcpStream,
        peer_kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
        peer_identifier: PeerIdentifier,
    ) -> Self {
        let (r, w) = stream.into_split();

        let read_connected = (r, peer_kind.clone()).into();
        let write_connected = (w, peer_kind.clone()).into();

        // Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let listening_actor =
            PeerListeningActor { read_connected, cluster_handler, self_id: peer_identifier };
        let listening_task = tokio::spawn(listening_actor.listen(kill_switch));

        Self {
            w_conn: write_connected,
            listner_kill_trigger: ListeningActorKillTrigger::new(kill_trigger, listening_task),
            last_seen: std::time::Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct WriteConnected {
    pub stream: OwnedWriteHalf,
    pub kind: PeerKind,
}

#[derive(Debug)]
pub(super) struct ReadConnected {
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
