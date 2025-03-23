use crate::domains::{
    cluster_actors::replication::ReplicationState, peers::identifier::PeerIdentifier,
};

#[derive(Debug)]
pub enum ConsensusClientResponse {
    LogIndex(Option<u64>),
    Err(String),
}

#[derive(Debug, Clone, PartialEq, bincode::Decode, bincode::Encode)]
pub struct ReplicationResponse {
    pub(crate) log_idx: u64,
    pub(crate) term: u64,
    pub(crate) is_granted: bool,
    pub(crate) from: PeerIdentifier,
}

impl ReplicationResponse {
    pub(crate) fn new(log_idx: u64, is_granted: bool, repl_state: &ReplicationState) -> Self {
        Self { log_idx, term: repl_state.term, is_granted, from: repl_state.self_identifier() }
    }
}
