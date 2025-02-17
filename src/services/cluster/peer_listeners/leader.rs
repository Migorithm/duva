use bytes::Bytes;

use crate::{services::cluster::peers::connected_types::FromMaster, SnapshotLoader};

use super::*;

impl TListen for PeerListener<FromMaster> {
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_master_stream() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
}

impl PeerListener<FromMaster> {
    async fn listen_master_stream(&mut self) {
        while let Ok(cmds) = self.read_command::<RequestFromMaster>().await {
            for cmd in cmds {
                match cmd {
                    RequestFromMaster::HeartBeat(mut state) => {
                        self.log_entries(&mut state).await;

                        self.receive_heartbeat(state).await;
                    }
                    RequestFromMaster::FullSync(data) => {
                        let Ok(snapshot) = SnapshotLoader::load_from_bytes(&data) else {
                            println!("[ERROR] Failed to load snapshot from master");
                            continue;
                        };
                        let Ok(_) = self.snapshot_applier.apply_snapshot(snapshot).await else {
                            println!("[ERROR] Failed to apply snapshot from master");
                            continue;
                        };
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum RequestFromMaster {
    HeartBeat(HeartBeatMessage),
    FullSync(Bytes),
}

impl TryFrom<QueryIO> for RequestFromMaster {
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
