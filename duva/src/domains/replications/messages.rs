use std::fmt::Display;

use crate::{
    domains::peers::command::{BatchEntries, BatchId, HeartBeat},
    prelude::PeerIdentifier,
};

#[derive(Clone, Debug, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub enum PeerMessage {
    AppendEntriesRPC(HeartBeat),
    ClusterHeartBeat(HeartBeat),
    AckReplication(ReplicationAck),
    RequestVote(RequestVote),
    ElectionVote(ElectionVote),
    StartRebalance,
    BatchEntries(BatchEntries),
    MigrationBatchAck(BatchId),
    CloseConnection,
}

#[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct RequestVote {
    pub(crate) term: u64, // current term of the candidate. Without it, the old leader wouldn't be able to step down gracefully.
    pub(crate) candidate_id: PeerIdentifier,
    pub(crate) last_log_index: u64,
    pub(crate) last_log_term: u64, //the term of the last log entry, used for election restrictions. If the term is low, it won't win the election.
}

#[derive(Clone, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct ElectionVote {
    pub(crate) term: u64,
    pub(crate) vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub struct ReplicationAck {
    pub(crate) log_idx: u64,
    pub(crate) term: u64,
    pub(crate) rej_reason: Option<RejectionReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, bincode::Decode, bincode::Encode)]
pub(crate) enum RejectionReason {
    ReceiverHasHigherTerm,
    LogInconsistency,
    FailToWrite,
}

impl ReplicationAck {
    pub(crate) fn ack(log_idx: u64, curr_term: u64) -> Self {
        Self { log_idx, term: curr_term, rej_reason: None }
    }

    pub(crate) fn reject(log_idx: u64, reason: RejectionReason, curr_term: u64) -> Self {
        Self { log_idx, term: curr_term, rej_reason: Some(reason) }
    }

    pub(crate) fn is_granted(&self) -> bool {
        self.rej_reason.is_none()
    }
}

#[derive(
    Debug, Clone, PartialEq, Default, Eq, PartialOrd, Ord, bincode::Encode, bincode::Decode, Hash,
)]
pub enum ReplicationId {
    #[default]
    Undecided,
    Key(String),
}

impl Display for ReplicationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationId::Undecided => write!(f, "?"),
            ReplicationId::Key(key) => write!(f, "{key}"),
        }
    }
}

impl From<ReplicationId> for String {
    fn from(value: ReplicationId) -> Self {
        match value {
            ReplicationId::Undecided => "?".to_string(),
            ReplicationId::Key(key) => key,
        }
    }
}

impl From<String> for ReplicationId {
    fn from(value: String) -> Self {
        match value.as_str() {
            "?" => ReplicationId::Undecided,
            _ => ReplicationId::Key(value),
        }
    }
}

impl From<ReplicationAck> for PeerMessage {
    fn from(value: ReplicationAck) -> Self {
        PeerMessage::AckReplication(value)
    }
}

impl From<RequestVote> for PeerMessage {
    fn from(value: RequestVote) -> Self {
        PeerMessage::RequestVote(value)
    }
}

impl From<ElectionVote> for PeerMessage {
    fn from(value: ElectionVote) -> Self {
        PeerMessage::ElectionVote(value)
    }
}

impl From<HeartBeat> for PeerMessage {
    fn from(value: HeartBeat) -> Self {
        PeerMessage::ClusterHeartBeat(value)
    }
}
impl From<BatchEntries> for PeerMessage {
    fn from(value: BatchEntries) -> Self {
        PeerMessage::BatchEntries(value)
    }
}
impl From<BatchId> for PeerMessage {
    fn from(value: BatchId) -> Self {
        PeerMessage::MigrationBatchAck(value)
    }
}
