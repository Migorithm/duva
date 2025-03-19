use crate::domains::{
    append_only_files::WriteOperation,
    cluster_actors::{
        commands::{RequestVote, RequestVoteReply},
        replication::HeartBeatMessage,
    },
    query_parsers::{QueryIO, deserialize},
};

#[derive(Debug)]
pub enum PeerInput {
    AppendEntriesRPC(HeartBeatMessage),
    ClusterHeartBeat(HeartBeatMessage),
    FullSync(Vec<WriteOperation>),
    Acks(Vec<u64>),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}

impl TryFrom<QueryIO> for PeerInput {
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
            QueryIO::AppendEntriesResponse(acks) => Ok(PeerInput::Acks(acks)),
            QueryIO::RequestVote(vote) => Ok(PeerInput::RequestVote(vote)),
            QueryIO::RequestVoteReply(reply) => Ok(PeerInput::RequestVoteReply(reply)),
            _ => Err(anyhow::anyhow!("Invalid data")),
        }
    }
}
