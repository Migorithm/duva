use crate::domains::{
    cluster_actors::{commands::ClusterCommand, replication::HeartBeatMessage},
    peers::{
        connected_types::ReadConnected,
        identifier::PeerIdentifier,
        peer::{ListeningActorKillTrigger, Peer, PeerKind},
    },
};

use listener::ClusterListener;
use peer_input::PeerInput;
use tokio::net::{TcpStream, tcp::OwnedReadHalf};
use tokio::select;

pub mod listener;
pub mod peer_input;

use crate::services::interface::TRead;

use tokio::sync::mpsc::Sender;
pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

// Listner requires cluster handler to send messages to the cluster actor and cluster actor instead needs kill trigger to stop the listener
pub(crate) fn create_peer(
    addr: String,
    stream: TcpStream,
    kind: PeerKind,
    cluster_handler: Sender<ClusterCommand>,
) -> Peer {
    let (r, w) = stream.into_split();
    let listening_actor =
        ClusterListener::new(ReadConnected::new(r), cluster_handler, addr.clone().into());

    let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
    let listener_kill_trigger: ListeningActorKillTrigger = ListeningActorKillTrigger::new(
        kill_trigger,
        tokio::spawn(listening_actor.listen(kill_switch)),
    );
    Peer::new(addr, w, kind, listener_kill_trigger)
}
