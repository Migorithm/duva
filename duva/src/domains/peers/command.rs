use crate::{
    ReplicationState,
    domains::{cluster_actors::replication::HeartBeat, query_parsers::QueryIO},
    prelude::PeerIdentifier,
};

pub(crate) use peer_messages::*;

#[derive(Debug)]
pub(crate) enum PeerListenerCommand {
    AppendEntriesRPC(HeartBeat),
    ClusterHeartBeat(HeartBeat),
    Acks(ReplicationResponse),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

impl TryFrom<QueryIO> for PeerListenerCommand {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::AppendEntriesRPC(peer_state) => Ok(Self::AppendEntriesRPC(peer_state)),
            QueryIO::ClusterHeartBeat(heartbeat) => Ok(Self::ClusterHeartBeat(heartbeat)),
            QueryIO::ConsensusFollowerResponse(acks) => Ok(PeerListenerCommand::Acks(acks)),
            QueryIO::RequestVote(vote) => Ok(PeerListenerCommand::RequestVote(vote)),
            QueryIO::RequestVoteReply(reply) => Ok(PeerListenerCommand::RequestVoteReply(reply)),
            _ => Err(anyhow::anyhow!("Invalid data")),
        }
    }
}

mod peer_messages {
    use super::*;
    #[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct RequestVote {
        pub(crate) term: u64, // current term of the candidate. Without it, the old leader wouldn't be able to step down gracefully.
        pub(crate) candidate_id: PeerIdentifier,
        pub(crate) last_log_index: u64,
        pub(crate) last_log_term: u64, //the term of the last log entry, used for election restrictions. If the term is low, it won’t win the election.
    }
    impl RequestVote {
        pub(crate) fn new(
            repl: &ReplicationState,
            last_log_index: u64,
            last_log_term: u64,
        ) -> Self {
            Self {
                term: repl.term,
                candidate_id: repl.self_identifier(),
                last_log_index,
                last_log_term,
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    pub struct RequestVoteReply {
        pub(crate) term: u64,
        pub(crate) vote_granted: bool,
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
}
