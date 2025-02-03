use crate::services::cluster::actors::listening_actor::ListeningActorKillTrigger;
use crate::services::cluster::actors::listening_actor::PeerListeningActor;
use crate::services::cluster::command::cluster_command::ClusterCommand;
use crate::services::cluster::peer::connected_types::WriteConnected;
use crate::services::cluster::peer::identifier::PeerIdentifier;
use crate::services::cluster::peer::kind::PeerKind;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

#[derive(Debug)]
pub(crate) struct Peer {
    pub(crate) w_conn: WriteConnected,
    pub(crate) listener_kill_trigger: ListeningActorKillTrigger,
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
            listener_kill_trigger: ListeningActorKillTrigger::new(kill_trigger, listening_task),
            last_seen: std::time::Instant::now(),
        }
    }
}

