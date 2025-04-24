use crate::domains::{
    cluster_actors::{
        commands::{ReplicationResponse, RequestVote, RequestVoteReply},
        replication::HeartBeatMessage,
    },
    operation_logs::WriteOperation,
    query_parsers::{QueryIO, deserialize},
};

#[derive(Debug)]
pub(crate) enum PeerListenerCommand {
    AppendEntriesRPC(HeartBeatMessage),
    ClusterHeartBeat(HeartBeatMessage),
    FullSync(Vec<WriteOperation>),
    Acks(ReplicationResponse),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

impl TryFrom<QueryIO> for PeerListenerCommand {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::File(data) => {
                let data = data.into();
                let Ok((QueryIO::Array(array), _)) = deserialize(data) else {
                    return Err(anyhow::anyhow!("Invalid data"));
                };
                let mut ops = Vec::new();
                for str in array {
                    let QueryIO::WriteOperation(log) = str else {
                        return Err(anyhow::anyhow!("Invalid data"));
                    };
                    ops.push(log);
                }
                Ok(Self::FullSync(ops))
            },
            QueryIO::AppendEntriesRPC(peer_state) => Ok(Self::AppendEntriesRPC(peer_state.0)),
            QueryIO::ClusterHeartBeat(heartbeat) => Ok(Self::ClusterHeartBeat(heartbeat.0)),
            QueryIO::ConsensusFollowerResponse(acks) => Ok(PeerListenerCommand::Acks(acks)),
            QueryIO::RequestVote(vote) => Ok(PeerListenerCommand::RequestVote(vote)),
            QueryIO::RequestVoteReply(reply) => Ok(PeerListenerCommand::RequestVoteReply(reply)),
            _ => Err(anyhow::anyhow!("Invalid data")),
        }
    }
}
