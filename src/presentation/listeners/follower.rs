use crate::domains::{
    append_only_files::log::LogIndex,
    cluster_actors::{
        commands::{ClusterCommand, RequestVote, RequestVoteReply},
        replication::HeartBeatMessage,
    },
    cluster_listeners::{ClusterListener, ReactorKillSwitch, TListen},
    peers::connected_types::Follower,
};

use super::*;

impl TListen for ClusterListener<Follower> {
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_replica_stream() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
}
impl ClusterListener<Follower> {
    async fn listen_replica_stream(&mut self) {
        while let Ok(cmds) = self.read_command::<FollowerInput>().await {
            for cmd in cmds {
                match cmd {
                    FollowerInput::HeartBeat(state) => {
                        self.receive_heartbeat(state).await;
                    },
                    FollowerInput::Acks(items) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::LeaderReceiveAcks(items))
                            .await;
                    },
                    FollowerInput::RequestVote(request_vote) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::VoteElection(request_vote))
                            .await;
                    },
                    FollowerInput::RequestVoteReply(reply) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::ApplyElectionVote(reply))
                            .await;
                    },
                }
            }
        }
    }
}

pub enum FollowerInput {
    HeartBeat(HeartBeatMessage),
    Acks(Vec<LogIndex>),
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
}
impl TryFrom<QueryIO> for FollowerInput {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::HeartBeat(peer_state) => Ok(FollowerInput::HeartBeat(peer_state)),
            QueryIO::Acks(acks) => Ok(FollowerInput::Acks(acks)),
            QueryIO::RequestVote(vote) => Ok(FollowerInput::RequestVote(vote)),
            QueryIO::RequestVoteReply(reply) => Ok(FollowerInput::RequestVoteReply(reply)),
            _ => todo!(),
        }
    }
}
