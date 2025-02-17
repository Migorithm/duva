pub mod communication_manager;
pub mod connection_manager;
pub mod inbound;
pub mod outbound;

use tokio::{net::TcpStream, sync::mpsc::Sender};

use crate::services::cluster::{
    actors::commands::ClusterCommand,
    peers::{
        connected_types::{Follower, Leader, NonDataPeer, WriteConnected},
        identifier::PeerIdentifier,
        kind::PeerKind,
        peer::Peer,
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

    match kind {
        PeerKind::Peer => Peer::new::<NonDataPeer>(
            WriteConnected { stream: w, kind },
            r,
            cluster_handler,
            addr,
            snapshot_applier,
        ),
        PeerKind::Replica => Peer::new::<Follower>(
            WriteConnected { stream: w, kind },
            r,
            cluster_handler,
            addr,
            snapshot_applier,
        ),
        PeerKind::Master => Peer::new::<Leader>(
            WriteConnected { stream: w, kind },
            r,
            cluster_handler,
            addr,
            snapshot_applier,
        ),
    }
}
