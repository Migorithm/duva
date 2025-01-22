use crate::services::cluster::actors::command::ClusterCommand;
use crate::services::cluster::actors::listening_actor::ListeningActorKillTrigger;
use crate::services::cluster::actors::listening_actor::PeerListeningActor;
use crate::services::cluster::actors::types::PeerKind;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: ClusterWriteConn,
    pub(crate) listner_kill_trigger: ListeningActorKillTrigger,
    state: PeerState,
}

impl Peer {
    pub fn new(
        stream: TcpStream,
        peer_kind: PeerKind,
        cluster_handler: Sender<ClusterCommand>,
    ) -> Self {
        let (r, w) = stream.into_split();

        let read_connected = (r, peer_kind.clone()).into();
        let write_connected = (w, peer_kind.clone()).into();

        // Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
        let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
        let listening_actor = PeerListeningActor { read_connected, cluster_handler };
        let listening_task = tokio::spawn(listening_actor.listen(kill_switch));

        Self {
            w_conn: write_connected,
            listner_kill_trigger: ListeningActorKillTrigger::new(kill_trigger, listening_task),
            state: PeerState::default(),
        }
    }
}

#[derive(Debug)]
pub struct ClusterWriteConn {
    pub stream: OwnedWriteHalf,
    pub kind: PeerKind,
}

#[derive(Debug)]
pub(super) struct ClusterReadConn {
    pub stream: OwnedReadHalf,
    pub kind: PeerKind,
}

#[derive(Debug, Default)]
struct PeerState {
    term: u64,
    offset: u64,
    last_updated: u64,
}

impl From<(OwnedReadHalf, PeerKind)> for ClusterReadConn {
    fn from((r, peer_kind): (OwnedReadHalf, PeerKind)) -> ClusterReadConn {
        ClusterReadConn { stream: r, kind: peer_kind }
    }
}

impl From<(OwnedWriteHalf, PeerKind)> for ClusterWriteConn {
    fn from((w, peer_kind): (OwnedWriteHalf, PeerKind)) -> ClusterWriteConn {
        ClusterWriteConn { stream: w, kind: peer_kind }
    }
}
