use crate::domains::{
    cluster_actors::replication::ReplicationState, peers::identifier::PeerIdentifier,
};

#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) struct RequestVote {
    pub(crate) term: u64, // current term of the candidate. Without it, the old leader wouldn't be able to step down gracefully.
    pub(crate) candidate_id: PeerIdentifier,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64, //the term of the last log entry, used for election restrictions. If the term is low, it won’t win the election.
}
impl RequestVote {
    pub(crate) fn new(repl: &ReplicationState, last_log_index: u64, last_log_term: u64) -> Self {
        Self {
            term: repl.term,
            candidate_id: repl.self_identifier(),
            last_log_index,
            last_log_term,
        }
    }
}

#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub(crate) struct RequestVoteReply {
    pub(crate) term: u64,
    pub(crate) vote_granted: bool,
}
