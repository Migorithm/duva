use super::replication::{HeartBeatMessage, ReplicationId, ReplicationState};
use crate::domains::append_only_files::WriteOperation;

use crate::domains::peers::peer::Peer;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

#[derive(Debug)]
pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationState>),

    SetReplicationInfo {
        replid: ReplicationId,
        hwm: u64,
    },
    InstallLeaderState(Vec<WriteOperation>),
    SendClusterHeatBeat,
    ForgetPeer(PeerIdentifier, tokio::sync::oneshot::Sender<Option<()>>),
    LeaderReqConsensus {
        log: WriteRequest,
        sender: tokio::sync::oneshot::Sender<WriteConsensusResponse>,
    },
    LeaderReceiveAcks(Vec<u64>),
    SendCommitHeartBeat {
        log_idx: u64,
    },
    AppendEntriesRPC(HeartBeatMessage),

    SendAppendEntriesRPC,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
    FetchCurrentState(tokio::sync::oneshot::Sender<Vec<WriteOperation>>),
    StartLeaderElection,
    VoteElection(RequestVote),
    ApplyElectionVote(RequestVoteReply),
    ClusterHeartBeat(HeartBeatMessage),
}

#[derive(Debug)]
pub enum WriteConsensusResponse {
    LogIndex(Option<u64>),
    Err(String),
}

#[derive(Debug)]
pub struct AddPeer {
    pub(crate) peer_id: PeerIdentifier,
    pub(crate) peer: Peer,
}

#[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
pub struct RequestVote {
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
pub struct RequestVoteReply {
    pub(crate) term: u64,
    pub(crate) vote_granted: bool,
}
