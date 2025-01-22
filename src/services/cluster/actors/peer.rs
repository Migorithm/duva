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
    pub(crate) w_conn: WriteConnected,
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
pub struct WriteConnected {
    pub stream: OwnedWriteHalf,
    pub kind: PeerKind,
}

#[derive(Debug)]
pub(super) struct ReadConnected {
    pub stream: OwnedReadHalf,
    pub kind: PeerKind,
}

#[derive(Debug, Default)]
pub(crate) struct PeerState {
    pub(crate) term: u64,
    pub(crate) offset: u64,
    pub(crate) last_updated: u64,
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

#[test]
fn test_peer_state_serialization() {
    use crate::services::query_io::QueryIO;

    //GIVEN
    let peer_state = PeerState { term: 1, offset: 2, last_updated: 3 };
    //WHEN
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();
    //THEN
    assert_eq!("^\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n", peer_state_serialized);

    //GIVEN
    let peer_state = PeerState { term: 5, offset: 3232, last_updated: 35535300 };
    //WHEN
    let peer_state_serialized: QueryIO = peer_state.into();
    let peer_state_serialized = peer_state_serialized.serialize();
    //THEN
    assert_eq!("^\r\n$1\r\n5\r\n$4\r\n3232\r\n$8\r\n35535300\r\n", peer_state_serialized);
}
