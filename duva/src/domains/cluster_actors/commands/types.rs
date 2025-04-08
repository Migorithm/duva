use crate::{
    domains::{
        append_only_files::WriteOperation,
        peers::{identifier::PeerIdentifier, peer::Peer},
    },
    from_to,
};

#[derive(Debug)]
pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}

#[derive(Debug)]
pub(crate) struct SyncLogs(pub(crate) Vec<WriteOperation>);
from_to!(Vec<WriteOperation>, SyncLogs);
