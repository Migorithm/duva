use crate::domains::{
    cluster_actors::{
        commands::{ReplicationResponse, RequestVote, RequestVoteReply},
        replication::HeartBeatMessage,
    },
    query_parsers::QueryIO,
};

#[derive(Debug)]
pub(crate) enum PeerListenerCommand {
    AppendEntriesRPC(HeartBeatMessage),
    ClusterHeartBeat(HeartBeatMessage),
    Acks(ReplicationResponse),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

impl TryFrom<QueryIO> for PeerListenerCommand {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::AppendEntriesRPC(peer_state) => Ok(Self::AppendEntriesRPC(peer_state.0)),
            QueryIO::ClusterHeartBeat(heartbeat) => Ok(Self::ClusterHeartBeat(heartbeat.0)),
            QueryIO::ConsensusFollowerResponse(acks) => Ok(PeerListenerCommand::Acks(acks)),
            QueryIO::RequestVote(vote) => Ok(PeerListenerCommand::RequestVote(vote)),
            QueryIO::RequestVoteReply(reply) => Ok(PeerListenerCommand::RequestVoteReply(reply)),
            _ => Err(anyhow::anyhow!("Invalid data")),
        }
    }
}
