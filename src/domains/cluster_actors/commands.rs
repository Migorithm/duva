use bincode::de;

use super::replication::{HeartBeatMessage, ReplicationState};
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::peers::peer::Peer;
use crate::domains::{append_only_files::WriteRequest, peers::identifier::PeerIdentifier};

#[derive(Debug)]
pub enum ClusterCommand {
    AddPeer(AddPeer),
    GetPeers(tokio::sync::oneshot::Sender<Vec<PeerIdentifier>>),
    ReplicationInfo(tokio::sync::oneshot::Sender<ReplicationState>),
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
    AppendEntriesRPC(HeartBeatMessage),

    SendLeaderHeartBeat,
    ClusterNodes(tokio::sync::oneshot::Sender<Vec<String>>),
    FetchCurrentState(tokio::sync::oneshot::Sender<Vec<WriteOperation>>),
    StartLeaderElection,
    VoteElection(RequestVote),
    ApplyElectionVote(RequestVoteReply),
    ClusterHeartBeat(HeartBeatMessage),
}

#[derive(Debug)]
pub enum WriteConsensusResponse {
    LogIndex(Option<LogIndex>),
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
    pub(crate) last_log_index: LogIndex,
    pub(crate) last_log_term: u64, //the term of the last log entry, used for election restrictions. If the term is low, it won’t win the election.
}
impl RequestVote {
    pub(crate) fn new(
        repl: &ReplicationState,
        last_log_index: LogIndex,
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

impl PartialEq for ClusterCommand {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::AddPeer(l0), Self::AddPeer(r0)) => true,
            (Self::GetPeers(l0), Self::GetPeers(r0)) => true,
            (Self::ReplicationInfo(l0), Self::ReplicationInfo(r0)) => true,
            (
                Self::SetReplicationInfo { leader_repl_id: l_leader_repl_id, hwm: l_hwm },
                Self::SetReplicationInfo { leader_repl_id: r_leader_repl_id, hwm: r_hwm },
            ) => l_leader_repl_id == r_leader_repl_id && l_hwm == r_hwm,
            (Self::InstallLeaderState(l0), Self::InstallLeaderState(r0)) => l0 == r0,
            (Self::ForgetPeer(l0, l1), Self::ForgetPeer(r0, r1)) => l0 == r0,
            (
                Self::LeaderReqConsensus { log: l_log, sender: l_sender },
                Self::LeaderReqConsensus { log: r_log, sender: r_sender },
            ) => l_log == r_log,
            (Self::LeaderReceiveAcks(l0), Self::LeaderReceiveAcks(r0)) => l0 == r0,
            (
                Self::SendCommitHeartBeat { log_idx: l_log_idx },
                Self::SendCommitHeartBeat { log_idx: r_log_idx },
            ) => l_log_idx == r_log_idx,
            (Self::AppendEntriesRPC(l0), Self::AppendEntriesRPC(r0)) => l0 == r0,
            (Self::ClusterNodes(l0), Self::ClusterNodes(r0)) => true,
            (Self::FetchCurrentState(l0), Self::FetchCurrentState(r0)) => true,
            (Self::VoteElection(l0), Self::VoteElection(r0)) => l0 == r0,
            (Self::ApplyElectionVote(l0), Self::ApplyElectionVote(r0)) => l0 == r0,
            (Self::ClusterHeartBeat(l0), Self::ClusterHeartBeat(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}
