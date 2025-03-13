use super::replication::{HeartBeatMessage, ReplicationInfo};
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::peers::peer::Peer;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationInfo>),
    SetReplicationInfo {
        leader_repl_id: PeerIdentifier,
        hwm: u64,
    },
    InstallLeaderState(Vec<WriteOperation>),
    SendHeartBeat,
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus {
        log: WriteRequest,
        sender: tokio::sync::oneshot::Sender<WriteConsensusResponse>,
    },
    LeaderReceiveAcks(Vec<LogIndex>),
    SendCommitHeartBeat {
        log_idx: LogIndex,
    },
    ReceiveHeartBeat(HeartBeatMessage),
    HandleLeaderHeartBeat(HeartBeatMessage),
    SendLeaderHeartBeat,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
    FetchCurrentState(tokio::sync::oneshot::Sender<Vec<WriteOperation>>),
    StartLeaderElection(tokio::sync::oneshot::Sender<bool>),
    VoteElection(RequestVote),
    ApplyElectionVote(RequestVoteReply),
}

#[derive(Debug)]
pub enum WriteConsensusResponse {
    LogIndex(Option<LogIndex>),
    Err(String),
}

pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}

#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct RequestVote {
    pub(crate) term: u64, // current term of the candidate. Without it, the old leader wouldn't be able to step down gracefully.
    pub(crate) candidate_id: PeerIdentifier,
    pub(crate) last_log_index: LogIndex,
    pub(crate) last_log_term: u64, //the term of the last log entry, used for election restrictions. If the term is low, it won’t win the election.
}
impl RequestVote {
    pub(crate) fn new(
        repl: &ReplicationInfo,
        last_log_index: LogIndex,
        last_log_term: u64,
    ) -> Self {
        Self {
            term: repl.term + 1,
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
