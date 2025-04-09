use crate::domains::{
    cluster_actors::commands::ClusterCommand,
    peers::{
        connected_types::ReadConnected,
        identifier::PeerIdentifier,
        peer::{ListeningActorKillTrigger, Peer, PeerState},
    },
};

use listener::ClusterListener;
use peer_input::PeerInput;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::select;

pub mod listener;
pub mod peer_input;

use crate::services::interface::TRead;

use tokio::sync::mpsc::Sender;
pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;

pub(crate) fn start_listen(
    r: OwnedReadHalf,
    addr: String,
    cluster_handler: Sender<ClusterCommand>,
) -> ListeningActorKillTrigger {
    let listening_actor =
        ClusterListener::new(ReadConnected::new(r), cluster_handler, addr.clone().into());

    let (kill_trigger, kill_switch) = tokio::sync::oneshot::channel();
    ListeningActorKillTrigger::new(kill_trigger, tokio::spawn(listening_actor.listen(kill_switch)))
}
