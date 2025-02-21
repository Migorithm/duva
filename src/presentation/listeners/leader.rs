use bytes::Bytes;

use crate::{
    SnapshotLoader,
    domains::{
        cluster_actors::replication::HeartBeatMessage,
        cluster_listeners::{ClusterListener, ReactorKillSwitch, TListen},
        peers::connected_types::Leader,
    },
};

use super::*;

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

impl ClusterListener<Leader> {
    async fn listen_leader(&mut self) {
        while let Ok(cmds) = self.read_command::<LeaderInput>().await {
            for cmd in cmds {
                match cmd {
                    LeaderInput::HeartBeat(state) => {
                        // TODO refactoring
                        self.accept_leader_hearbeat(state).await;
                    }
                    LeaderInput::FullSync(data) => {
                        let Ok(snapshot) = SnapshotLoader::load_from_bytes(&data) else {
                            println!("[ERROR] Failed to load snapshot from leader");
                            continue;
                        };
                        let Ok(_) = self.snapshot_applier.apply_snapshot(snapshot).await else {
                            println!("[ERROR] Failed to apply snapshot from leader");
                            continue;
                        };
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum LeaderInput {
    HeartBeat(HeartBeatMessage),
    FullSync(Bytes),
}

impl TryFrom<QueryIO> for LeaderInput {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::File(data) => Ok(Self::FullSync(data)),
            QueryIO::HeartBeat(peer_state) => Ok(Self::HeartBeat(peer_state)),
            // TODO term info should be included?
            _ => todo!(),
        }
    }
}
