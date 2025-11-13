use super::write_stream::MsgToServer;
use duva::domains::query_io::SERDE_CONFIG;

use duva::make_smart_pointer;
use duva::prelude::PeerIdentifier;
use duva::prelude::ReplicationId;
use duva::prelude::anyhow;
use duva::prelude::anyhow::Context;
use duva::prelude::bincode;
use duva::prelude::rand;
use duva::prelude::rand::SeedableRng;
use duva::prelude::rand::rngs::StdRng;
use duva::prelude::rand::seq::IteratorRandom;
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use duva::presentation::clients::request::ClientAction;
use duva::presentation::clients::request::SessionRequest;
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
    pub(crate) fn new(target_id: ReplicationId) -> Self {
        Self { conns: HashMap::new(), seed_node: target_id }
    }

    pub(crate) async fn remove_connection(
        &mut self,
        leader_id: &ReplicationId,
    ) -> anyhow::Result<PeerIdentifier> {
        let Some(connection) = self.conns.remove(leader_id) else {
            anyhow::bail!("Must be able to find connection {}", file!());
        };
        let peer_identifier = connection.peer_identifier.clone();
        connection.kill().await;
        Ok(peer_identifier)
    }
    pub(crate) async fn remove_outdated_connections(&mut self, node_repl_ids: Vec<ReplicationId>) {
        let outdated_connections = self.conns.extract_if(|repl_id, _| {
            !node_repl_ids.contains(repl_id) && repl_id != &self.seed_node
        });
        join_all(outdated_connections.into_iter().map(|(_, connection)| connection.kill())).await;
    }

    pub(crate) async fn randomized_send(&self, client_action: ClientAction) -> anyhow::Result<()> {
        let mut rng = StdRng::from_rng(&mut rand::rng());

        self.conns
            .values()
            .choose(&mut rng)
            .context("Connection set invalid! Random connection choice failed")?
            .send(client_action)
            .await?;
        Ok(())
    }

    pub(crate) async fn send_to_seed(&self, client_action: ClientAction) -> anyhow::Result<()> {
        self.send_to(&self.seed_node, client_action).await
    }

    pub(crate) async fn send_to(
        &self,
        node_id: &ReplicationId,
        client_action: ClientAction,
    ) -> anyhow::Result<()> {
        let connection = self.conns.get(node_id).context("No connections available")?;
        connection.send(client_action).await
    }

    pub(crate) async fn send_all(&self, client_action: ClientAction) -> anyhow::Result<usize> {
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
    pub(crate) peer_identifier: PeerIdentifier,
}

impl NodeConnection {
    async fn kill(self) {
        let _ = self.kill_switch.send(());
        let _ = self.writer.send(MsgToServer::Stop).await;
    }

    pub(crate) async fn send(&self, client_action: ClientAction) -> anyhow::Result<()> {
        let session_request =
            SessionRequest { conn_offset: self.request_id, action: client_action };
        self.writer
            .send(MsgToServer::Command(bincode::encode_to_vec(session_request, SERDE_CONFIG)?))
            .await
            .context("Failed to send commend")
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
        let mut connections = NodeConnections::new(repl_id.clone());
        connections.insert(
            repl_id.clone(),
            NodeConnection {
                writer: tx,
                kill_switch: kill_tx,
                request_id: 0,
                peer_identifier: PeerIdentifier::default(),
            },
        );

        // When
        connections.remove_connection(&repl_id).await.unwrap();

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
        let conn = NodeConnection {
            writer: tx1,
            kill_switch: kill_tx1,
            request_id: 0,
            peer_identifier: PeerIdentifier::default(),
        };
        let mut connections = NodeConnections::new(repl_id1.clone());
        connections.insert(repl_id1.clone(), conn);

        let (tx2, _rx2) = mpsc::channel(10);
        let (kill_tx2, _kill_rx2) = oneshot::channel();
        connections.insert(
            repl_id2.clone(),
            NodeConnection {
                writer: tx2,
                kill_switch: kill_tx2,
                request_id: 0,
                peer_identifier: PeerIdentifier::default(),
            },
        );

        // When - peer2 is not in the topology peers, repl_id1 is kept
        let topology_peers = vec![repl_id1.clone()];
        connections.remove_outdated_connections(topology_peers).await;

        // Then
        assert_eq!(connections.len(), 1);
        assert!(connections.contains_key(&repl_id1));
        assert!(!connections.contains_key(&repl_id2));
    }

    #[tokio::test]
    async fn test_leader_connections_remove_outdated_connections_except_seed() {
        // Given
        let repl_id1 = ReplicationId::Key("key1".into());
        let repl_id2 = ReplicationId::Key("key2".into());
        let (tx1, _rx1) = mpsc::channel(10);
        let (kill_tx1, _kill_rx1) = oneshot::channel();
        let conn = NodeConnection {
            writer: tx1,
            kill_switch: kill_tx1,
            request_id: 0,
            peer_identifier: PeerIdentifier::default(),
        };
        let mut connections = NodeConnections::new(repl_id1.clone());
        connections.insert(repl_id1.clone(), conn);

        let (tx2, _rx2) = mpsc::channel(10);
        let (kill_tx2, _kill_rx2) = oneshot::channel();
        connections.insert(
            repl_id2.clone(),
            NodeConnection {
                writer: tx2,
                kill_switch: kill_tx2,
                request_id: 0,
                peer_identifier: PeerIdentifier::default(),
            },
        );

        // When - peer1 is seed, both should be kept
        let topology_peers = vec![repl_id2.clone()];
        connections.remove_outdated_connections(topology_peers).await;

        // Then
        assert_eq!(connections.len(), 2);
        assert!(connections.contains_key(&repl_id1));
        assert!(connections.contains_key(&repl_id2));
    }
}
