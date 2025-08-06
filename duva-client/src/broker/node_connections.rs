use super::write_stream::MsgToServer;
use duva::domains::IoError;
use duva::domains::caches::cache_manager::IndexedValueCodec;
use duva::domains::cluster_actors::replication::ReplicationId;
use duva::domains::query_io::QueryIO;
use duva::domains::query_io::QueryIO::SessionRequest;
use duva::make_smart_pointer;
use duva::prelude::rand;
use duva::prelude::rand::seq::IteratorRandom;
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use duva::presentation::clients::request::ClientAction;
use futures::future::join_all;
use futures::future::try_join_all;
use std::collections::HashMap;

make_smart_pointer!(NodeConnections, HashMap<ReplicationId,NodeConnection> =>conns);

#[derive(Debug)]
pub(crate) struct NodeConnections {
    conns: HashMap<ReplicationId, NodeConnection>,
    seed_node: ReplicationId,
}

impl NodeConnections {
    pub(crate) fn new(target_id: ReplicationId, connection: NodeConnection) -> Self {
        let mut connections = HashMap::new();
        connections.insert(target_id.clone(), connection);
        Self { conns: connections, seed_node: target_id }
    }

    pub(crate) fn is_seed_node(&self, repl_id: &ReplicationId) -> bool {
        self.seed_node.clone() == repl_id.clone()
    }

    pub(crate) fn update_seed_node(&mut self) -> Result<(), IoError> {
        self.seed_node = self.get_random_connection()?.0.clone();
        Ok(())
    }

    pub(crate) async fn remove_connection(&mut self, leader_id: &ReplicationId) {
        if let Some(connection) = self.conns.remove(leader_id) {
            connection.kill().await;
        }
    }
    pub(crate) async fn remove_outdated_connections(&mut self, node_repl_ids: Vec<ReplicationId>) {
        let outdated_connections =
            self.conns.extract_if(|repl_id, _| !node_repl_ids.contains(repl_id));
        join_all(outdated_connections.into_iter().map(|(_, connection)| connection.kill())).await;
    }

    pub(crate) async fn randomized_send(&self, client_action: ClientAction) -> Result<(), IoError> {
        // ThreadRng internally uses thread-local storage, so the actual RNG state is reused per thread
        self.get_random_connection()?.1.send(client_action).await
    }

    fn get_random_connection(&self) -> Result<(&ReplicationId, &NodeConnection), IoError> {
        self.conns
            .iter()
            .choose(&mut rand::rng())
            .ok_or(IoError::Custom("No connections available".to_string()))
    }

    pub(crate) async fn send_to_seed(&self, client_action: ClientAction) -> Result<(), IoError> {
        self.send_to(&self.seed_node, client_action).await
    }

    pub(crate) async fn send_to(
        &self,
        node_id: &ReplicationId,
        client_action: ClientAction,
    ) -> Result<(), IoError> {
        let connection = self
            .conns
            .get(node_id)
            .ok_or(IoError::Custom("No connections available".to_string()))?;
        connection.send(client_action).await
    }

    pub(crate) async fn send_all(&self, client_action: ClientAction) -> Result<usize, IoError> {
        try_join_all(self.keys().map(|node_id| self.send_to(node_id, client_action.clone())))
            .await?;
        Ok(self.len())
    }
}

#[derive(Debug)]
pub(crate) struct NodeConnection {
    pub(crate) writer: mpsc::Sender<MsgToServer>,
    pub(crate) kill_switch: oneshot::Sender<()>,
    pub(crate) request_id: u64,
}

impl NodeConnection {
    // ! CONSIDER IDEMPOTENCY RULE
    // !
    // ! If request is updating action yet receive error, we need to increase the request id
    // ! otherwise, server will not be able to process the next command
    pub(crate) fn update_request_id(&mut self, query_io: &QueryIO) {
        match query_io {
            // * Current rule: s:value-idx:index_num
            | QueryIO::SimpleString(v) => {
                let s = String::from_utf8_lossy(v);
                self.request_id = IndexedValueCodec::decode_index(s)
                    .filter(|&id| id > self.request_id)
                    .unwrap_or(self.request_id);
            },

            //TODO replace "self.request_id + 1" - make the call to get "current_index" from the server
            | QueryIO::Err(_) => self.request_id = self.request_id + 1,
            | _ => {},
        }
    }

    async fn kill(self) {
        let _ = self.kill_switch.send(());
        let _ = self.writer.send(MsgToServer::Stop).await;
    }

    pub(crate) async fn send(&self, client_action: ClientAction) -> Result<(), IoError> {
        let session_request = SessionRequest { request_id: self.request_id, client_action };
        self.writer
            .send(MsgToServer::Command(session_request.serialize().to_vec()))
            .await
            .map_err(|e| IoError::Custom(format!("Failed to send command: {e}")))
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
        let mut connections = NodeConnections::new(
            repl_id.clone(),
            NodeConnection { writer: tx, kill_switch: kill_tx, request_id: 0 },
        );

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
        let conn = NodeConnection { writer: tx1, kill_switch: kill_tx1, request_id: 0 };
        let mut connections = NodeConnections::new(repl_id1.clone(), conn);

        let (tx2, _rx2) = mpsc::channel(10);
        let (kill_tx2, _kill_rx2) = oneshot::channel();
        connections.insert(
            repl_id2.clone(),
            NodeConnection { writer: tx2, kill_switch: kill_tx2, request_id: 0 },
        );

        // When - peer1 is not in the topology peers, repl_id2 is kept
        let topology_peers = vec![repl_id2.clone()];
        connections.remove_outdated_connections(topology_peers).await;

        // Then
        assert_eq!(connections.len(), 1);
        assert!(!connections.contains_key(&repl_id1));
        assert!(connections.contains_key(&repl_id2));
    }
}
