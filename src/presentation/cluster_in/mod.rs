pub mod communication_manager;
pub mod connection_manager;
pub mod inbound;
pub mod outbound;
pub mod peer_listener;
pub mod peer_listeners;

use std::marker::PhantomData;

use peer_listener::PeerListener;
use tokio::{net::TcpStream, sync::mpsc::Sender};

use crate::services::cluster::{
    actors::commands::ClusterCommand,
    peers::{
        connected_types::{FromMaster, FromPeer, FromSlave, ReadConnected, WriteConnected},
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
    cluster_handler: Sender<ClusterCommand>,
    snapshot_applier: SnapshotApplier,
) -> Peer {
    let (r, w) = stream.into_split();

    let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();

    match kind {
        PeerKind::Peer => {
            let rc = ReadConnected::<FromPeer>::new(r);
            let listening_actor = PeerListener::new(rc, cluster_handler, addr, snapshot_applier);
            let listener_kill_trigger = ListeningActorKillTrigger::new(
                kill_trigger,
                tokio::spawn(listening_actor.listen(kill_switch)),
            );
            Peer::new(WriteConnected { stream: w, kind }, listener_kill_trigger)
        }
        PeerKind::Replica => {
            let rc = ReadConnected::<FromSlave>::new(r);
            let listening_actor = PeerListener::new(rc, cluster_handler, addr, snapshot_applier);

            let listener_kill_trigger = ListeningActorKillTrigger::new(
                kill_trigger,
                tokio::spawn(listening_actor.listen(kill_switch)),
            );
            Peer::new(WriteConnected { stream: w, kind }, listener_kill_trigger)
        }
        PeerKind::Master => {
            let rc = ReadConnected::<FromMaster>::new(r);
            let listening_actor = PeerListener::new(rc, cluster_handler, addr, snapshot_applier);
            let listener_kill_trigger = ListeningActorKillTrigger::new(
                kill_trigger,
                tokio::spawn(listening_actor.listen(kill_switch)),
            );
            Peer::new(WriteConnected { stream: w, kind }, listener_kill_trigger)
        }
    }
}
