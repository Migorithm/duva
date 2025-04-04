use crate::domains::{
    cluster_actors::replication::ReplicationState, peers::identifier::PeerIdentifier,
};

#[derive(Debug)]
pub(crate) enum ConsensusClientResponse {
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
pub(crate) enum RejectionReason {
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

    pub(crate) fn is_granted(&self) -> bool {
        self.rej_reason == RejectionReason::None
    }

    #[cfg(test)]
    pub(crate) fn set_from(self, from: &str) -> Self {
        Self { from: PeerIdentifier(from.to_string()), ..self }
    }
}
