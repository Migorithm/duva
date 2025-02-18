use crate::services::cluster::peers::connected_types::NonDataPeer;

use super::*;

impl TListenClusterPeer for ClusterListener<NonDataPeer> {
    async fn listen(mut self, rx: ReactorKillSwitch) -> OwnedReadHalf {
        let connected = select! {
            _ = self.listen_peer_stream() => self.read_connected.stream,
            // If the kill switch is triggered, return the connected stream so the caller can decide what to do with it
            _ = rx => self.read_connected.stream
        };
        connected
    }
}

impl ClusterListener<NonDataPeer> {
    async fn listen_peer_stream(&mut self) {
        while let Ok(values) = self.read_connected.stream.read_values().await {
            let _ = values;
        }
    }
}
