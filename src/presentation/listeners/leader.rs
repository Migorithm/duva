use super::*;
use crate::domains::append_only_files::WriteOperation;
use crate::domains::cluster_actors::commands::ClusterCommand;
use crate::domains::cluster_actors::replication::HeartBeatMessage;
use crate::domains::cluster_listeners::ClusterListener;
use crate::domains::cluster_listeners::ReactorKillSwitch;
use crate::domains::cluster_listeners::TListen;
use crate::domains::peers::connected_types::Leader;
use crate::domains::query_parsers::deserialize;

impl TListen for ClusterListener<Leader> {
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_leader() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
}

#[cfg(test)]
static ATOMIC: std::sync::atomic::AtomicI16 = std::sync::atomic::AtomicI16::new(0);

impl ClusterListener<Leader> {
    async fn listen_leader(&mut self) {
        while let Ok(cmds) = self.read_command::<LeaderInput>().await {
            #[cfg(test)]
            ATOMIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for cmd in cmds {
                match cmd {
                    LeaderInput::HeartBeat(state) => {
                        println!("[INFO] from {}, hc:{}", state.heartbeat_from, state.hop_count);
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::HandleLeaderHeartBeat(state))
                            .await;
                    },
                    LeaderInput::FullSync(logs) => {
                        println!("Received full sync logs: {:?}", logs);
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::InstallLeaderState(logs))
                            .await;
                    },
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum LeaderInput {
    HeartBeat(HeartBeatMessage),
    FullSync(Vec<WriteOperation>),
}

impl TryFrom<QueryIO> for LeaderInput {
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
            _ => todo!(),
        }
    }
}
