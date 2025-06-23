use super::write_stream::MsgToServer;
use duva::prelude::{anyhow, anyhow::anyhow};
use duva::prelude::tokio::sync::mpsc;
use duva::prelude::tokio::sync::oneshot;
use duva::prelude::PeerIdentifier;
use std::collections::HashMap;

pub(crate) struct LeaderConnections {
    main_leader_id: PeerIdentifier,
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
        main_leader_id: PeerIdentifier,
        writer: mpsc::Sender<MsgToServer>,
        kill_switch: oneshot::Sender<()>,
    ) -> Self {
        let mut connections = HashMap::new();
        connections.insert(main_leader_id.clone(), LeaderConnection { writer, kill_switch });
        Self { main_leader_id, connections }
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

    pub(crate) fn remove_main_connection(
        &mut self,
    ) -> Option<LeaderConnection> {
        self.connections.remove(&self.main_leader_id)
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

    pub(crate) fn main_leader_id(&self) -> &PeerIdentifier {
        &self.main_leader_id
    }

    pub(crate) fn remove_connection(&mut self, leader_id: &PeerIdentifier) -> Option<LeaderConnection> {
        self.connections.remove(leader_id)
    }

    pub(crate) fn is_main_leader(&self, leader_id: &PeerIdentifier) -> bool {
        leader_id == &self.main_leader_id
    }
}
