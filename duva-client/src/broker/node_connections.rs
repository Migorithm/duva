use super::write_stream::MsgToServer;
use duva::domains::cluster_actors::replication::ReplicationId;
use duva::prelude::anyhow;
use duva::prelude::anyhow::anyhow;
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct NodeConnections {
    connections: HashMap<ReplicationId, NodeConnection>,
}

#[derive(Debug)]
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
        target_id: ReplicationId,
        writer: mpsc::Sender<MsgToServer>,
        kill_switch: oneshot::Sender<()>,
        request_id: u64,
    ) -> Self {
        let mut connections = HashMap::new();
        connections.insert(target_id.clone(), NodeConnection::new(writer, kill_switch, request_id));
        Self { connections }
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = &ReplicationId> {
        self.connections.keys()
    }
    pub(crate) fn contains_key(&self, leader_id: &ReplicationId) -> bool {
        self.connections.contains_key(leader_id)
    }

    pub(crate) fn get_first_node_id(&self) -> anyhow::Result<&ReplicationId> {
        self.connections.keys().next().ok_or_else(|| anyhow!("No connections available"))
    }

    pub(crate) fn get(&self, leader_id: &ReplicationId) -> anyhow::Result<&NodeConnection> {
        self.connections
            .get(leader_id)
            .ok_or(anyhow!("Connection not found for leader_id: {}", leader_id))
    }

    pub(crate) fn get_mut(
        &mut self,
        leader_id: &ReplicationId,
    ) -> anyhow::Result<&mut NodeConnection> {
        self.connections
            .get_mut(leader_id)
            .ok_or(anyhow!("Connection not found for leader_id: {}", leader_id))
    }

    pub(crate) fn insert(&mut self, leader_id: ReplicationId, connection: NodeConnection) {
        self.connections.insert(leader_id, connection);
    }

    pub(crate) async fn remove_connection(&mut self, leader_id: &ReplicationId) {
        if let Some(connection) = self.connections.remove(leader_id) {
            let _ = connection.kill_switch.send(());
            let _ = connection.writer.send(MsgToServer::Stop).await;
        }
    }
    pub(crate) async fn remove_outdated_connections(&mut self, node_repl_ids: Vec<ReplicationId>) {
        let outdated_connections =
            self.connections.extract_if(|repl_id, _| !node_repl_ids.contains(repl_id));
        for (_, connection) in outdated_connections {
            let _ = connection.kill_switch.send(());
            let _ = connection.writer.send(MsgToServer::Stop).await;
        }
    }

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
        let repl_id = ReplicationId::Key("Some".into());
        let (tx, _rx) = mpsc::channel(10);
        let (kill_tx, _kill_rx) = oneshot::channel();
        let mut connections = NodeConnections::new(repl_id.clone(), tx, kill_tx, 0);

        // When
        connections.remove_connection(&repl_id).await;

        // Then
        assert!(connections.is_empty());
        assert!(!connections.contains_key(&repl_id));
    }

    #[tokio::test]
    async fn test_leader_connections_remove_outdated_connections() {
        // Given
        let repl_id1 = ReplicationId::Key("key1".into());
        let repl_id2 = ReplicationId::Key("key2".into());
        let (tx1, _rx1) = mpsc::channel(10);
        let (kill_tx1, _kill_rx1) = oneshot::channel();
        let mut connections = NodeConnections::new(repl_id1.clone(), tx1, kill_tx1, 0);

        let (tx2, _rx2) = mpsc::channel(10);
        let (kill_tx2, _kill_rx2) = oneshot::channel();
        connections.insert(repl_id2.clone(), NodeConnection::new(tx2, kill_tx2, 0));

        // When - peer1 is not in the topology peers, repl_id2 is kept
        let topology_peers = vec![repl_id2.clone()];
        connections.remove_outdated_connections(topology_peers).await;

        // Then
        assert_eq!(connections.len(), 1);
        assert!(!connections.contains_key(&repl_id1));
        assert!(connections.contains_key(&repl_id2));
    }
}
