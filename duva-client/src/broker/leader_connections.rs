use super::write_stream::MsgToServer;
use duva::prelude::{anyhow, anyhow::anyhow};
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use duva::prelude::PeerIdentifier;
use std::collections::HashMap;

pub(crate) struct LeaderConnections {
    connections: HashMap<PeerIdentifier, LeaderConnection>,
}

pub(crate) struct LeaderConnection {
    pub(crate) writer: mpsc::Sender<MsgToServer>,
    pub(crate) kill_switch: oneshot::Sender<()>,
}

impl LeaderConnection {
    pub(crate) async fn send(&self, msg: MsgToServer) -> Result<(), mpsc::error::SendError<MsgToServer>> {
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
        self.connections.iter().next()
            .map(|(_, conn)| conn)
            .expect("No connections available")
    }

    pub(crate) fn add_connection(
        &mut self,
        leader_id: PeerIdentifier,
        writer: mpsc::Sender<MsgToServer>,
        kill_switch: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        if self.connections.contains_key(&leader_id) {
            return Err(anyhow!("Connection to leader {} already exists", leader_id));
        }
        self.connections.insert(leader_id, LeaderConnection { writer, kill_switch });
        Ok(())
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

    pub(crate) fn remove_connection(&mut self, leader_id: &PeerIdentifier) -> Option<LeaderConnection> {
        self.connections.remove(leader_id)
    }

    pub(crate) fn remove_outdated_connections(&mut self, node_peer_ids: Vec<PeerIdentifier>) {
        self.connections.retain(|peer_id, _| node_peer_ids.contains(peer_id));
    }
}
