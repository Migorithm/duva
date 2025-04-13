use crate::domains::peers::{
    connected_types::ReadConnected, identifier::PeerIdentifier, peer::ListeningActorKillTrigger,
};
use peer_input::PeerInput;
use tokio::net::tcp::OwnedReadHalf;
use tokio::select;

pub mod listener;
pub mod peer_input;

use crate::services::interface::TRead;

use tokio::sync::mpsc::Sender;
pub(crate) type ReactorKillSwitch = tokio::sync::oneshot::Receiver<()>;
