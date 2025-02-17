use crate::services::cluster::peers::connected_types::FromSlave;

use super::*;

impl TListen for PeerListener<FromSlave> {
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_replica_stream() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
}
impl PeerListener<FromSlave> {
    async fn listen_replica_stream(&mut self) {
        while let Ok(cmds) = self.read_command::<RequestFromSlave>().await {
            for cmd in cmds {
                match cmd {
                    RequestFromSlave::HeartBeat(state) => {
                        self.receive_heartbeat(state).await;
                    }
                    RequestFromSlave::Acks(items) => {
                        let _ = self
                            .cluster_handler
                            .send(ClusterCommand::LeaderReceiveAcks(items))
                            .await;
                    }
                }
            }
        }
    }
}

pub enum RequestFromSlave {
    HeartBeat(HeartBeatMessage),
    Acks(Vec<u64>),
}
impl TryFrom<QueryIO> for RequestFromSlave {
    type Error = anyhow::Error;
    fn try_from(query: QueryIO) -> anyhow::Result<Self> {
        match query {
            QueryIO::HeartBeat(peer_state) => Ok(RequestFromSlave::HeartBeat(peer_state)),
            QueryIO::Acks(acks) => Ok(RequestFromSlave::Acks(acks)),
            _ => todo!(),
        }
    }
}
