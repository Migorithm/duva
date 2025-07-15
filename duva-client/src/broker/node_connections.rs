use super::write_stream::MsgToServer;
use duva::prelude::anyhow::anyhow;
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use duva::prelude::{PeerIdentifier, anyhow};
use std::collections::HashMap;

pub(crate) struct NodeConnections {
    connections: HashMap<PeerIdentifier, NodeConnection>,
}

pub(crate) struct NodeConnection {
    pub(crate) writer: mpsc::Sender<MsgToServer>,
    pub(crate) kill_switch: oneshot::Sender<()>,
    pub(crate) request_id: u64,
}

impl NodeConnection {
    pub(crate) fn new(
        writer: mpsc::Sender<MsgToServer>,
        kill_switch: oneshot::Sender<()>,
        request_id: u64,
    ) -> Self {
        Self { writer, kill_switch, request_id }
    }

    pub(crate) async fn send(
        &self,
        msg: MsgToServer,
    ) -> Result<(), mpsc::error::SendError<MsgToServer>> {
        self.writer.send(msg).await
    }
}

impl NodeConnections {
    pub(crate) fn new(
        target_id: PeerIdentifier,
        writer: mpsc::Sender<MsgToServer>,
        kill_switch: oneshot::Sender<()>,
        request_id: u64,
    ) -> Self {
        let mut connections = HashMap::new();
        connections.insert(target_id.clone(), NodeConnection::new(writer, kill_switch, request_id));
        Self { connections }
    }

    pub(crate) fn contains_key(&self, leader_id: &PeerIdentifier) -> bool {
        self.connections.contains_key(leader_id)
    }

    pub(crate) fn get_first_node_id(&self) -> anyhow::Result<&PeerIdentifier> {
        self.connections.keys().next().ok_or_else(|| anyhow!("No connections available"))
    }

    pub(crate) fn get(&self, leader_id: &PeerIdentifier) -> anyhow::Result<&NodeConnection> {
        Ok(self
            .connections
            .get(leader_id)
            .ok_or(anyhow!("Connection not found for leader_id: {}", leader_id))?)
    }

    pub(crate) fn get_mut(
        &mut self,
        leader_id: &PeerIdentifier,
    ) -> anyhow::Result<&mut NodeConnection> {
        Ok(self
            .connections
            .get_mut(leader_id)
            .ok_or(anyhow!("Connection not found for leader_id: {}", leader_id))?)
    }

    pub(crate) fn insert(&mut self, leader_id: PeerIdentifier, connection: NodeConnection) {
        self.connections.insert(leader_id, connection);
    }

    pub(crate) async fn remove_connection(&mut self, leader_id: &PeerIdentifier) {
        if let Some(connection) = self.connections.remove(leader_id) {
            let _ = connection.kill_switch.send(());
            let _ = connection.writer.send(MsgToServer::Stop).await;
        }
    }
    pub(crate) async fn remove_outdated_connections(&mut self, node_peer_ids: Vec<PeerIdentifier>) {
        let outdated_connections =
            self.connections.extract_if(|peer_id, _| !node_peer_ids.contains(peer_id));
        for (_, connection) in outdated_connections {
            let _ = connection.kill_switch.send(());
            let _ = connection.writer.send(MsgToServer::Stop).await;
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.connections.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duva::prelude::tokio;
    use duva::prelude::tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn test_leader_connections_remove_connection() {
        // Given
        let peer_id = PeerIdentifier::new("localhost", 3333);
        let (tx, _rx) = mpsc::channel(10);
        let (kill_tx, _kill_rx) = oneshot::channel();
        let mut connections = NodeConnections::new(peer_id.clone(), tx, kill_tx, 0);

        // When
        connections.remove_connection(&peer_id).await;

        // Then
        assert!(connections.is_empty());
        assert!(!connections.contains_key(&peer_id));
    }

    #[tokio::test]
    async fn test_leader_connections_remove_outdated_connections() {
        // Given
        let peer1 = PeerIdentifier::new("localhost", 3333);
        let peer2 = PeerIdentifier::new("localhost", 4444);
        let (tx1, _rx1) = mpsc::channel(10);
        let (kill_tx1, _kill_rx1) = oneshot::channel();
        let mut connections = NodeConnections::new(peer1.clone(), tx1, kill_tx1, 0);

        let (tx2, _rx2) = mpsc::channel(10);
        let (kill_tx2, _kill_rx2) = oneshot::channel();
        connections.insert(peer2.clone(), NodeConnection::new(tx2, kill_tx2, 0));

        // When - peer1 is not in the topology peers, peer2 is kept
        let topology_peers = vec![peer2.clone()];
        connections.remove_outdated_connections(topology_peers).await;

        // Then
        assert_eq!(connections.len(), 1);
        assert!(!connections.contains_key(&peer1));
        assert!(connections.contains_key(&peer2));
    }
}
