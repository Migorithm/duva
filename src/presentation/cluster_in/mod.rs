pub mod inbound;
pub mod manager;
pub mod outbound;
pub mod peer_listeners;

use peer_listeners::peer_listener::PeerListeningActor;
use tokio::{net::TcpStream, sync::mpsc::Sender};

use crate::services::cluster::{
    command::cluster_command::ClusterCommand,
    peers::{
        connected_types::{ReadConnected, WriteConnected},
        identifier::PeerIdentifier,
        kind::PeerKind,
        peer::Peer,
    },
};
fn create_peer(
    stream: TcpStream,
    kind: PeerKind,
    addr: PeerIdentifier,
    cluster_actor_handler: Sender<ClusterCommand>,
) -> Peer {
    let (r, w) = stream.into_split();

    let read_connected = ReadConnected { stream: r, kind: kind.clone() };
    let listener_kill_trigger =
        PeerListeningActor::new(read_connected, cluster_actor_handler, addr);

    let peer = Peer::new(WriteConnected { stream: w, kind }, listener_kill_trigger);
    peer
}
