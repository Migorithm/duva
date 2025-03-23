use crate::domains::peers::{identifier::PeerIdentifier, peer::Peer};

#[derive(Debug)]
pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}
