use super::*;
use crate::domains::append_only_files::WriteOperation;
use crate::domains::append_only_files::log::LogIndex;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::commands::RequestVote;
use crate::domains::cluster_actors::commands::RequestVoteReply;
use crate::domains::cluster_actors::replication::HeartBeatMessage;
use crate::domains::cluster_listeners::ClusterListener;
use crate::domains::cluster_listeners::ReactorKillSwitch;

use crate::domains::query_parsers::deserialize;

#[cfg(test)]
static ATOMIC: std::sync::atomic::AtomicI16 = std::sync::atomic::AtomicI16::new(0);

impl ClusterListener {
    pub(crate) async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_peer() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
    async fn listen_peer(&mut self) {
        while let Ok(cmds) = self.read_command().await {
            #[cfg(test)]
            ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for cmd in cmds {
                match cmd {
                    PeerInput::HeartBeat(state) => {
                        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::ReceiveHeartBeat(state))
                            .await;
                    },
                    PeerInput::FullSync(logs) => {
                        println!("Received full sync logs: {:?}", logs);
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::InstallLeaderState(logs))
                            .await;
                    },
                    PeerInput::Acks(items) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::LeaderReceiveAcks(items))
                            .await;
                    },
                    PeerInput::RequestVote(request_vote) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::VoteElection(request_vote))
                            .await;
                    },
                    PeerInput::RequestVoteReply(reply) => {
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

#[derive(Debug)]
pub enum PeerInput {
    HeartBeat(HeartBeatMessage),
    FullSync(Vec<WriteOperation>),
    Acks(Vec<LogIndex>),
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
            QueryIO::HeartBeat(peer_state) => Ok(Self::HeartBeat(peer_state)),
            QueryIO::Acks(acks) => Ok(PeerInput::Acks(acks)),
            QueryIO::RequestVote(vote) => Ok(PeerInput::RequestVote(vote)),
            QueryIO::RequestVoteReply(reply) => Ok(PeerInput::RequestVoteReply(reply)),
            _ => todo!(),
        }
    }
}
