use super::write_stream::MsgToServer;
use duva::prelude::PeerIdentifier;
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use std::collections::HashMap;

pub(crate) struct LeaderConnections {
    connections: HashMap<PeerIdentifier, LeaderConnection>,
}

pub(crate) struct LeaderConnection {
    pub(crate) writer: mpsc::Sender<MsgToServer>,
    pub(crate) kill_switch: oneshot::Sender<()>,
}

impl LeaderConnection {
    pub(crate) async fn send(
        &self,
        msg: MsgToServer,
    ) -> Result<(), mpsc::error::SendError<MsgToServer>> {
        self.writer.send(msg).await
    }
}

impl LeaderConnections {
    pub(crate) fn new(
        target_id: PeerIdentifier,
        writer: mpsc::Sender<MsgToServer>,
        kill_switch: oneshot::Sender<()>,
    ) -> Self {
        let mut connections = HashMap::new();
        connections.insert(target_id.clone(), LeaderConnection { writer, kill_switch });
        Self { connections }
    }

    pub(crate) fn first(&self) -> &LeaderConnection {
        self.connections.iter().next().map(|(_, conn)| conn).expect("No connections available")
    }

    pub(crate) fn contains_key(&self, leader_id: &PeerIdentifier) -> bool {
        self.connections.contains_key(leader_id)
    }

    pub(crate) fn get(&self, leader_id: &PeerIdentifier) -> Option<&LeaderConnection> {
        self.connections.get(leader_id)
    }

    pub(crate) fn insert(
        &mut self,
        leader_id: PeerIdentifier,
        connection: (oneshot::Sender<()>, mpsc::Sender<MsgToServer>),
    ) {
        let (kill_switch, writer) = connection;
        self.connections.insert(leader_id, LeaderConnection { writer, kill_switch });
    }

    pub(crate) async fn remove_connection(&mut self, leader_id: &PeerIdentifier) {
        if let Some(connection) = self.connections.remove(leader_id) {
            println!("Removing connection to {}", leader_id);
            let _ = connection.kill_switch.send(());
            let _ = connection.writer.send(MsgToServer::Stop).await;
        }
    }

    pub(crate) async fn remove_outdated_connections(&mut self, node_peer_ids: Vec<PeerIdentifier>) {
        let outdated_connections =
            self.connections.extract_if(|peer_id, _| !node_peer_ids.contains(peer_id));
        for (peer_id, connection) in outdated_connections {
            println!("Removing outdated connection to {}", peer_id);
            let _ = connection.kill_switch.send(());
            let _ = connection.writer.send(MsgToServer::Stop).await;
        }
        println!("Remaining connections: {:?}", self.connections.keys());
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &LeaderConnection> {
        self.connections.values()
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (&PeerIdentifier, &LeaderConnection)> {
        self.connections.iter()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.connections.len()
    }
}

#[test]
fn test_remove_outdated_connections() {
    // Given
    let peer1 = PeerIdentifier::new("localhost", 3333);
    let writer = mpsc::channel(100).0;
    let reader = oneshot::channel().0;
    let mut connections = LeaderConnections::new(peer1, writer, reader);

    // When
    // peer1 is not in the topology peers
    let topology_peers =
        vec![PeerIdentifier::new("localhost", 2222), PeerIdentifier::new("localhost", 4444)];
    connections.remove_outdated_connections(topology_peers);

    // Then
    assert!(connections.is_empty())
}
