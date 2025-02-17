pub mod communication_manager;
pub mod connection_manager;
pub mod inbound;
pub mod outbound;
pub mod peer_listener;
pub mod peer_listeners;

use peer_listener::PeerListener;
use tokio::{net::TcpStream, sync::mpsc::Sender};

use crate::services::cluster::{
    actors::commands::ClusterCommand,
    peers::{
        connected_types::{ReadConnected, WriteConnected},
        identifier::PeerIdentifier,
        kind::PeerKind,
        peer::{ListeningActorKillTrigger, Peer},
    },
};
use crate::services::statefuls::snapshot::snapshot_applier::SnapshotApplier;

fn create_peer(
    stream: TcpStream,
    kind: PeerKind,
    addr: PeerIdentifier,
    cluster_actor_handler: Sender<ClusterCommand>,
    snapshot_applier: SnapshotApplier,
) -> Peer {
    let (r, w) = stream.into_split();

    let read_connected = ReadConnected { stream: r, kind: kind.clone() };

    let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();

    let listening_actor = PeerListener {
        read_connected,
        cluster_handler: cluster_actor_handler,
        self_id: addr,
        snapshot_applier,
    };

    let listener_kill_trigger = ListeningActorKillTrigger::new(
        kill_trigger,
        tokio::spawn(listening_actor.listen(kill_switch)),
    );
    let peer = Peer::new(WriteConnected { stream: w, kind }, listener_kill_trigger);
    peer
}
