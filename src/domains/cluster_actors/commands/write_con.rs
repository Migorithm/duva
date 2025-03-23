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
    pub(crate) rej_reason: RejectionReason,
    pub(crate) from: PeerIdentifier,
}

#[derive(Debug, Clone, PartialEq, bincode::Decode, bincode::Encode)]
pub enum RejectionReason {
    ReceiverHasHigherTerm,
    LogInconsistency,
    None,
}

impl ReplicationResponse {
    pub(crate) fn new(
        log_idx: u64,
        rej_reason: RejectionReason,
        repl_state: &ReplicationState,
    ) -> Self {
        Self { log_idx, term: repl_state.term, rej_reason, from: repl_state.self_identifier() }
    }
}
