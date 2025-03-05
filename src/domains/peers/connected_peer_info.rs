use super::identifier::PeerIdentifier;

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectedPeerInfo {
    // TODO repl_id here is the leader_replid from connected server.
    pub(crate) id: PeerIdentifier,
    pub(crate) leader_repl_id: PeerIdentifier,
    pub(crate) offset: u64,
    pub(crate) peer_list: Vec<String>,
}

impl ConnectedPeerInfo {
    pub(crate) fn list_peer_binding_addrs(&mut self) -> Vec<PeerIdentifier> {
        std::mem::take(&mut self.peer_list).into_iter().map(Into::into).collect::<Vec<_>>()
    }
}
