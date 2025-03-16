use super::listeners::ClusterListener;
use crate::domains::{
    cluster_actors::commands::ClusterCommand,
    peers::{
        connected_types::ReadConnected,
        peer::{ListeningActorKillTrigger, Peer, PeerKind},
    },
};
use tokio::{net::TcpStream, sync::mpsc::Sender};
pub mod communication_manager;
pub mod connection_manager;
pub mod inbound;
pub mod outbound;

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
